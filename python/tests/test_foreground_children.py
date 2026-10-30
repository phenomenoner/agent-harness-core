import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_foreground_runtime import setup

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.cli import foreground_children as composition
from ahc.cli.foreground_children import ChildEnabledBackend, ForegroundChildren
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError


def test_close_retires_successful_siblings_and_retains_only_failed_owner():
    children = ForegroundChildren(object(), object())
    parent, workspace = object(), object()
    children.children, children.workspace = parent, workspace
    calls, failing = [], [True]

    def first():
        calls.append("first")
        if failing[0]:
            raise RuntimeError("private cleanup detail")

    def second():
        calls.append("second")

    children.services = {"first": SimpleNamespace(close=first),
                         "second": SimpleNamespace(close=second)}
    with pytest.raises(ContractError, match="^child cleanup remains pending$"):
        children.close()
    assert calls == ["first", "second"]
    assert set(children.services) == {"first"}
    assert children.children is parent and children.workspace is workspace
    failing[0] = False
    children.close()
    children.close()
    assert calls == ["first", "second", "first"]
    assert children.children is children.workspace is None and not children.services


@pytest.mark.parametrize("close_failure", [False, True])
@pytest.mark.parametrize("kind", ["subagent", "minion"])
def test_foreground_child_tool_admission_mailbox_and_cleanup(tmp_path, monkeypatch, close_failure,
                                                           kind):
    runtime, requests, _, request = setup(tmp_path)
    effects, results = [], []
    failing = [close_failure]
    owner = SimpleNamespace(config=SimpleNamespace(to_dict=lambda: {
        "native_minion": {"profile": {"enabled": True, "environment_names": []}}}))
    children = ForegroundChildren(runtime.store, owner if kind == "minion" else object())
    runtime.children = children

    class Factory:
        def __init__(self, owner, ledger, envelope, token, workspace):
            self.ledger, self.envelope, self.token = ledger, envelope, token

        def port(self):
            owner = self

            class Port:
                def run(self, envelope, *, cancelled):
                    assert not cancelled()
                    with owner.ledger.backend_admission(envelope, owner.token):
                        effects.append("child-run")
                    return "child answer"

                def close(self):
                    effects.append("child-close")
                    if failing[0]:
                        raise ContractError("controlled close failure")
            return Port()

    monkeypatch.setattr(composition, "TrustedCodexChildFactory", Factory)
    if kind == "minion":
        from ahc.cli import native_child_factory

        def native(value, ledger, envelope, token, *, environment, workspace):
            assert envelope.execution_kind == "minion" and environment == {}
            return Factory(owner, ledger, envelope, token, None).port()
        monkeypatch.setattr(native_child_factory, "ForegroundNativeChild", native)

    def factory(bound, tools, *, cancelled):
        class Backend:
            def invoke(self, admitted):
                adapter = CodexWorkspaceTools(admitted, tools, "thread", "turn")
                declarations = adapter.declarations()[0]["tools"]
                declaration = next(row for row in declarations if row["name"] == "child")
                assert set(declaration["inputSchema"]["required"]) == {
                    "expected", "child_id", "task_text", "timeout_ms"}
                args = {"expected": tools.workspace.identity.to_dict(), "child_id": "worker",
                        "task_text": "bounded task", "timeout_ms": 10000}
                if kind == "minion":
                    args["execution_kind"] = kind
                missing = {key: value for key, value in args.items() if key != "timeout_ms"}
                rejected = tools.invoke("workspace.child", missing)
                assert rejected["reason"] == "invalid-child-request"
                assert rejected["missing_fields"] == ["timeout_ms"]
                assert rejected["request_admitted"] is False
                assert effects == []
                message = {"id": 1, "method": "item/tool/call", "params": {
                    "threadId": "thread", "turnId": "turn", "namespace": "workspace",
                    "tool": "child", "callId": "first", "arguments": args}}
                reply = adapter.respond(message)
                result = json.loads(reply["result"]["contentItems"][0]["text"])
                results.append(result)
                assert tools.invoke("workspace.child", args) == result
                assert tools.invoke("workspace.child", {**args, "task_text": "changed"})[
                    "disposition"] == "rejected"
                stale = {**args, "expected": {**args["expected"], "revision": 99}}
                assert tools.invoke("workspace.child", stale)["disposition"] == "rejected"
                return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                    output_text="parent answer", unresolved_effects=result["unresolved_effects"])

            def close(self):
                effects.append("parent-close")
        return ChildEnabledBackend(Backend(), children, bound, tools.workspace, cancelled)

    runtime.backend_factory = factory
    runtime.start()
    try:
        modes = runtime.child_status()
        assert modes["subagent"]["status"] == "configured"
        assert modes["minion"] == {"status": "configured" if kind == "minion" else "absent",
            "reason_code": None if kind == "minion" else "native-minion-adapter-not-configured",
            "execution_verified": False, "fallback": None}
        requests.append(replace(request, capabilities=runtime.tools.capabilities()))
        if close_failure:
            assert not runtime.step().result["accepted"]
            assert runtime._backend is not None and "parent-close" not in effects
            assert runtime.step() is None
            assert runtime.cleanup_status()["state"] == "exhausted"
            failing[0] = False
        else:
            assert runtime.step().result["accepted"]
            assert results[0]["result"]["result"] == {"output_text": "child answer"}
            assert children.children is None
        assert effects.count("child-run") == 1
        assert runtime.store.read_turn(request.session, results[0]["result"]["child_id"]) is None
    finally:
        failing[0] = False
        runtime.close()
    assert effects[-2:] == ["child-close", "parent-close"]
