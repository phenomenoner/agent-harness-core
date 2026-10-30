import json
from types import SimpleNamespace

import pytest
from test_foreground_config_v2 import valid_config
from test_workspace_tools import setup

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.application.child_dispatch import ChildDispatchService
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.cli.trusted_codex_child import TrustedCodexChildFactory
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_artifacts import ChildArtifacts
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository


def test_opted_in_codex_binding_bridge_and_typed_terminal(tmp_path, monkeypatch):
    store, workspace, _ = setup(tmp_path)
    current = store.read_current("session")
    raw = valid_config()
    raw["backend"]["enabled"] = True
    raw["child_limits"] = {"max_children": 2, "max_concurrency": 1,
                           "max_artifacts": 1, "artifact_bytes": 100}
    config = parse_foreground_config_v2(raw)
    owner = TrustedCodexBackendFactory(store, config)
    parent_tools = SimpleNamespace(workspace=workspace,
                                   capabilities=lambda: ("workspace.artifact",))
    artifact, binding = owner.binding(parent_tools)
    parent = AgentTurnRequest(current.session, "parent", current.goal.digest, "work", (),
        artifact, binding, "work", 100, capabilities=("workspace.artifact",),
        effective_policy=config.effective_policy)
    children = ChildDispatchRepository(store, parent, clock=lambda: 100,
                                       monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "report", "work", 10000)
    factory = TrustedCodexChildFactory(owner, children, envelope, "claim", workspace.identity)
    closed = []

    class Backend:
        def invoke(self, request):
            assert request.capabilities == ("workspace.publish_text", "workspace.attach")
            bridge = CodexWorkspaceTools(request, factory.tools, "thread", "turn", clock=lambda: 0)
            names = [v["name"] for v in bridge.declarations()[0]["tools"]]
            assert names == ["publish_text", "attach"]

            def call(name, index, **arguments):
                reply = bridge.respond({"id": index, "method": "item/tool/call", "params": {
                    "threadId": "thread", "turnId": "turn", "namespace": "workspace",
                    "tool": name, "callId": str(index), "arguments": {
                        "expected": workspace.identity.to_dict(), **arguments}}})
                assert reply["result"]["success"] is True
                return json.loads(reply["result"]["contentItems"][0]["text"])["result"]

            value = call("publish_text", 1, text="report", media_type="text/plain")
            call("attach", 2, artifact_id=value["artifact_id"], filename="report.txt")
            with pytest.raises(ContractError):
                call("execute", 3, code="42")
            return AgentTurnObservation(request, "completed", "controlled", 1, 1,
                output_text="done", artifacts=bridge.artifacts)

        def close(self):
            closed.append(True)

    monkeypatch.setattr(factory, "_backend", lambda *args, **kwargs: Backend())
    try:
        with store.execution_lock():
            store.begin(current, parent)
            children.admit(envelope)
            ChildArtifacts(children).allocate(envelope.parent, "child", max_count=1, max_bytes=100)
            with store.workspace_turn_scope(parent):
                result = ChildDispatchService(children, lambda item: factory.port()).dispatch(
                    envelope, "claim")
            assert result["state"] == "terminal"
            assert result["result"]["artifacts"][0]["filename"] == "report.txt"
        assert closed == [True]
    finally:
        workspace.close()
