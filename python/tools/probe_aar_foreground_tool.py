"""Actual native tool-dispatch composition with an in-memory workspace context."""

import json
import os
from types import SimpleNamespace


def dispatch_tool(value, store, parent, environment):
    from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
    from ahc.cli.foreground_children import ForegroundChildren
    from ahc.domain.workspace import WorkspaceIdentity
    from ahc.infrastructure.native_child_repository import NativeChildRepository

    ports = []

    class InstrumentedChildren(ForegroundChildren):
        def _port(self, envelope):
            port = super()._port(envelope)
            ports.append(port)
            return port

    workspace = SimpleNamespace(store=store,
                                identity=WorkspaceIdentity(parent.session, "workspace"))
    owner = SimpleNamespace(config=SimpleNamespace(to_dict=lambda: {"native_minion": value}))
    children = InstrumentedChildren(store, owner)
    tools = SimpleNamespace(workspace=workspace, capabilities=lambda: ("workspace.child",),
        invoke=lambda name, arguments: children.invoke(workspace.identity, arguments))
    previous = {name: os.environ.get(name) for name in environment}
    os.environ.update(environment)
    try:
        children.bind(parent, workspace, lambda: False)
        adapter = CodexWorkspaceTools(parent, tools, "fixture-thread", "fixture-turn")
        arguments = {"expected": workspace.identity.to_dict(), "child_id": "native-tool-child",
                     "task_text": "Return a fixture result.", "timeout_ms": 10000,
                     "execution_kind": "minion"}

        def invoke(call_id, args):
            reply = adapter.respond({"id": call_id, "method": "item/tool/call", "params": {
                "threadId": "fixture-thread", "turnId": "fixture-turn", "namespace": "workspace",
                "tool": "child", "callId": call_id, "arguments": args}})
            return json.loads(reply["result"]["contentItems"][0]["text"])

        result = invoke("first", arguments)
        assert result["disposition"] == "observed", result
        assert result["result"]["state"] == "terminal", result
        if value.get("retain_artifacts", False):
            from ahc.infrastructure.artifact_repository import ArtifactRepository
            from ahc.infrastructure.runtime_repository import RuntimeRepository

            artifacts = result["result"]["artifacts"]
            assert len(artifacts) == 1
            manifest, content = ArtifactRepository(RuntimeRepository(store.root)).read(
                parent.session, artifacts[0]["artifact_id"])
            assert manifest.byte_count == len(content) > 0
            assert b"aar.rlm.v1" in content
        assert invoke("repeat", arguments) == result
        assert invoke("wrong-mode", {**arguments, "execution_kind": "subagent"})[
            "disposition"] == "rejected"
        second = invoke("second", {**arguments, "child_id": "over-budget"})
        assert second["disposition"] == "unknown"
        assert len(ports) == 2 and ports[1].client is None
        child_id = result["result"]["child_id"]
        native = NativeChildRepository(children.children)
        receipt = native.read(children.children.parent, child_id)
        assert receipt["receipt"].state == "succeeded"
        with store._read() as connection:
            _, envelope, _ = children.children._row(
                connection, children.children.parent, child_id)
            assert connection.execute(
                "SELECT count(*) FROM runtime_native_budgets").fetchone()[0] == 1
        return envelope, result["result"], receipt
    finally:
        try:
            children.close()
            for port in ports:
                assert port.closed
                if port.client is not None:
                    assert port.client.transport.wire.owned.process.poll() is not None
        finally:
            for name, old in previous.items():
                if old is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = old
