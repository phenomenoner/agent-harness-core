"""Actual dynamic tool bridge through foreground composition and real child ledger."""

import json
from dataclasses import replace
from threading import Barrier
from types import SimpleNamespace

import pytest
from test_child_worker_repository import prepared

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.application.workspace_tools import WorkspaceTools
from ahc.cli import foreground_children as composition
from ahc.domain.workspace import WorkspaceIdentity


@pytest.mark.parametrize("count", [3, 16, 19])
@pytest.mark.parametrize("deadline", [100, 1200])
def test_dynamic_batch_tool_runs_and_replays_without_new_factories(tmp_path, monkeypatch, count, deadline):
    store, request, ledger, _ = prepared(tmp_path)
    request = replace(request, deadline=deadline)
    workspace = SimpleNamespace(store=store, closed=False,
        identity=WorkspaceIdentity(request.session, "workspace"))
    owner = SimpleNamespace(config=SimpleNamespace(to_dict=lambda: {
        "child_limits": {"max_children": 32, "max_concurrency": 3}}))
    children = composition.ForegroundChildren(store, owner)
    children.bind(request, workspace, lambda: False)
    children.children.clock = lambda: 100
    children.children.monotonic_clock = lambda: 0
    tools = WorkspaceTools(workspace, children=children)
    calls, closed = [], []
    barrier = Barrier(3, timeout=5) if count == 3 else None

    class Factory:
        def __init__(self, owner, worker, envelope, token, identity):
            self.worker, self.envelope, self.token = worker, envelope, token

        def port(self):
            owner = self
            calls.append(owner.envelope.child_id)
            expected_deadline = 100 + min(900000, int(request.deadline * 1000) - 1)
            assert owner.envelope.deadline_unix_ms == expected_deadline

            class Port:
                def run(self, item, *, cancelled):
                    with owner.worker.backend_admission(item, owner.token):
                        if barrier is not None:
                            barrier.wait()
                        return "中" * 1000

                def close(self):
                    closed.append(owner.envelope.child_id)

            return Port()

    monkeypatch.setattr(composition, "TrustedCodexChildFactory", Factory)
    admitted = replace(request, capabilities=("workspace.child_batch",))
    adapter = CodexWorkspaceTools(admitted, tools, "thread", "turn", clock=lambda: 0)
    declaration = adapter.declarations()[0]["tools"][0]
    assert declaration["name"] == "child_batch"
    assert "maximum" not in declaration["inputSchema"]["properties"]["concurrency"]
    assert "maximum" not in declaration["inputSchema"]["properties"]["children"]["items"]["properties"]["timeout_ms"]
    args = {"expected": workspace.identity.to_dict(), "concurrency": 3,
            "children": [{"child_id": str(i), "task_text": "work", "timeout_ms": 900000}
                         for i in range(count)]}
    with store.execution_lock(), store.workspace_turn_scope(request):
        reply = adapter.respond({"id": 1, "method": "item/tool/call", "params": {
            "threadId": "thread", "turnId": "turn", "namespace": "workspace",
            "tool": "child_batch", "callId": "batch", "arguments": args}})
        result = json.loads(reply["result"]["contentItems"][0]["text"])
        assert reply["result"]["success"] and result["disposition"] == "observed"
        assert len(result["result"]) == count
        assert all(item["result"]["result"]["output_truncated"] for item in result["result"])
        assert tools.invoke("workspace.child_batch", args) == {
            key: value for key, value in result.items() if key != "tool_budget"}
    children.close()
    assert len(calls) == len(closed) == count
    assert all(row["result"]["output_text"] == "中" * 1000 for row in ledger.read(ledger.parent))


@pytest.mark.parametrize("invalid", ["stale", "duplicate", "native", "last_bad"])
def test_invalid_batch_rejects_entire_request_without_ledger_writes(tmp_path, invalid):
    store, request, ledger, _ = prepared(tmp_path)
    workspace = SimpleNamespace(store=store, closed=False,
        identity=WorkspaceIdentity(request.session, "workspace"))
    children = composition.ForegroundChildren(store, object())
    children.bind(request, workspace, lambda: False)
    tools = WorkspaceTools(workspace, children=children)
    item = {"child_id": "one", "task_text": "work", "timeout_ms": 1000}
    children.children.clock = lambda: 100
    children.children.monotonic_clock = lambda: 0
    args = {"expected": workspace.identity.to_dict(), "concurrency": 3, "children": [item]}
    if invalid == "stale":
        args["expected"]["revision"] += 1
    elif invalid == "duplicate":
        args["children"].append(item.copy())
    elif invalid == "native":
        item["execution_kind"] = "minion"
    else:
        args["children"].append({**item, "child_id": "two", "timeout_ms": True})
    with store.execution_lock(), store.workspace_turn_scope(request):
        assert tools.invoke("workspace.child_batch", args)["disposition"] == "rejected"
    assert ledger.read(ledger.parent) == ()
    children.close()
