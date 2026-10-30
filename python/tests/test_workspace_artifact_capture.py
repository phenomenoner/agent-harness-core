"""Capture admitted namespace output into durable artifacts and model metadata."""

import json
from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository


class Port:
    def __init__(self, identity, _checkpoint):
        self.identity, self.values, self.closed = identity, {}, False

    def execute(self, expected, code):
        exec(code, self.values)
        self.identity = replace(expected, revision=expected.revision + 1)
        return {
            "identity": self.identity.to_dict(),
            "ok": True,
            "output": "",
            "output_truncated": False,
        }

    def export_artifact(self, expected, name):
        assert expected == self.identity
        return self.values[name]

    def close(self):
        self.closed = True


def test_admitted_capture_returns_manifest_only_and_persists_namespace_bytes(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    artifacts = ArtifactRepository(repo)
    tools = WorkspaceTools(workspace, artifacts=artifacts)
    try:
        old = workspace.identity
        workspace.execute(old, "report = b'captured namespace bytes'")
        args = {
            "expected": workspace.identity.to_dict(),
            "name": "report",
            "media_type": "application/octet-stream",
            "lineage": [],
        }
        assert (
            tools.invoke("workspace.artifact", {**args, "expected": old.to_dict()})["disposition"]
            == "rejected"
        )
        bound = replace(request, capabilities=tools.capabilities(), workspace_scope="work")
        declarations = CodexWorkspaceTools.describe(bound, tools)[0]["tools"]
        matching = [value for value in declarations if value["name"] == "artifact"]
        assert len(matching) == 1
        declaration = matching[0]
        assert set(declaration["inputSchema"]["required"]) == set(args)
        bridge = CodexWorkspaceTools(bound, tools, "thread", "turn", clock=lambda: 0)
        response = bridge.respond(
            {
                "id": 1,
                "method": "item/tool/call",
                "params": {
                    "threadId": "thread",
                    "turnId": "turn",
                    "namespace": "workspace",
                    "tool": "artifact",
                    "callId": "capture",
                    "arguments": args,
                },
            }
        )
        result = json.loads(response["result"]["contentItems"][0]["text"])
        assert result["disposition"] == "observed"
        assert "captured namespace bytes" not in json.dumps(response)
        manifest, content = ArtifactRepository(RuntimeRepository(repo.root)).read(
            request.session, result["result"]["artifact_id"]
        )
        assert content == b"captured namespace bytes"
        assert manifest.producer == workspace.identity
        assert workspace.identity.revision == 1
        with pytest.raises(ContractError):
            bridge.respond(
                {
                    "id": 2,
                    "method": "item/tool/call",
                    "params": {
                        "threadId": "thread",
                        "turnId": "turn",
                        "namespace": "workspace",
                        "tool": "artifact",
                        "callId": "capture",
                        "arguments": args,
                    },
                }
            )
    finally:
        workspace.close()


def test_unsupported_port_does_not_advertise_capture(tmp_path):
    from test_workspace_tools import setup as workspace_setup

    repo, workspace, _ = workspace_setup(tmp_path)
    try:
        tools = WorkspaceTools(workspace, artifacts=ArtifactRepository(repo))
        assert "workspace.artifact" not in tools.capabilities()
    finally:
        workspace.close()


def test_known_export_rejection_does_not_poison_following_useful_work(tmp_path):
    class KnownRejectionPort(Port):
        def export_artifact(self, expected, name):
            if name == 'unsupported':
                raise ContractError('known unsupported primitive value')
            return super().export_artifact(expected, name)
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, 'work', KnownRejectionPort)
    artifacts = ArtifactRepository(repo)
    try:
        tools = WorkspaceTools(workspace, artifacts=artifacts)
        bound = replace(request, capabilities=tools.capabilities(), workspace_scope='work')
        bridge = CodexWorkspaceTools(bound, tools, 'thread', 'turn', clock=lambda: 0)
        def invoke(identifier, operation, arguments):
            reply = bridge.respond({'id': identifier, 'method': 'item/tool/call', 'params': {
                'threadId': 'thread', 'turnId': 'turn', 'namespace': 'workspace',
                'tool': operation, 'callId': 'call-'+str(identifier), 'arguments': arguments}})
            return json.loads(reply['result']['contentItems'][0]['text'])
        rejected = invoke(1, 'artifact', {'expected': workspace.identity.to_dict(),
            'name': 'unsupported', 'media_type': 'application/zip', 'lineage': []})
        assert rejected['disposition'] == 'rejected'
        assert rejected['unresolved_effects'] is False and bridge.unresolved is False
        executed = invoke(2, 'execute', {'expected': workspace.identity.to_dict(),
                                        'code': "summary=b'confirmed useful fallback'"})
        assert executed['disposition'] == 'observed'
        captured = invoke(3, 'artifact', {'expected': workspace.identity.to_dict(),
            'name': 'summary', 'media_type': 'text/plain', 'lineage': []})
        assert captured['disposition'] == 'observed'
        assert artifacts.read(request.session, captured['result']['artifact_id'])[1] == b'confirmed useful fallback'
        assert bridge.unresolved is False and workspace.closed is False
    finally:
        workspace.close()


def test_exception_after_actual_publication_remains_unknown_without_replay(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, 'work', Port)
    artifacts = ArtifactRepository(repo)
    class LostAcknowledgement:
        store = repo
        calls = 0
        def publish(self, expected, content, media_type, lineage=()):
            self.calls += 1
            artifacts.publish(expected, content, media_type, lineage=lineage)
            raise ContractError('receipt lost after durable insert')
    publisher = LostAcknowledgement()
    try:
        workspace.execute(workspace.identity, "report=b'published once' ")
        result = WorkspaceTools(workspace, artifacts=publisher).invoke('workspace.artifact', {
            'expected': workspace.identity.to_dict(), 'name': 'report',
            'media_type': 'text/plain', 'lineage': []})
        assert result['disposition'] == 'unknown' and result['unresolved_effects'] is True
        assert publisher.calls == 1
        with repo._read() as db:
            assert db.execute('SELECT count(*) FROM runtime_artifacts').fetchone()[0] == 1
    finally:
        workspace.close()


def test_untyped_export_fault_cannot_claim_unknown_publication_and_fences_only_bad_workspace(tmp_path):
    class BrokenExportPort(Port):
        def export_artifact(self, expected, name):
            raise RuntimeError("PRIVATE corrupt export transport")

    repo, snapshot, _ = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", BrokenExportPort)
    try:
        tools = WorkspaceTools(workspace, artifacts=ArtifactRepository(repo))
        response = tools.invoke("workspace.artifact", {"expected": workspace.identity.to_dict(),
            "name": "report", "media_type": "text/plain", "lineage": []})
        assert response["disposition"] == "rejected" and not response["unresolved_effects"]
        assert response["workspace_available"] is False and workspace.unavailable
        assert workspace.port.closed and tools.capabilities() == ()
        assert "PRIVATE" not in json.dumps(response)
        with repo._read() as connection:
            assert connection.execute("SELECT count(*) FROM runtime_artifacts").fetchone()[0] == 0
    finally:
        workspace.close()


def test_optional_learning_fault_keeps_the_successful_tool_receipt(tmp_path):
    from types import SimpleNamespace

    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    def failed_capture(*_args):
        raise RuntimeError("PRIVATE experience store fault")
    try:
        tools = WorkspaceTools(workspace, learning=SimpleNamespace(record_tool=failed_capture))
        request = replace(request, capabilities=tools.capabilities(), workspace_scope="work")
        bridge = CodexWorkspaceTools(request, tools, "thread", "turn", clock=lambda: 0)
        response = bridge.respond({"id": 1, "method": "item/tool/call", "params": {
            "threadId": "thread", "turnId": "turn", "namespace": "workspace", "tool": "execute",
            "callId": "one", "arguments": {"expected": workspace.identity.to_dict(), "code": "value=42"}}})
        assert response["result"]["success"] and not bridge.unresolved
        assert workspace.port.values["value"] == 42
        assert "PRIVATE" not in json.dumps(response)
    finally:
        workspace.close()
