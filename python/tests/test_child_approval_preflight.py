import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_codex_client import setup
from test_codex_workspace_tools import message
from test_runtime_approval_store import setup as approval_setup

from ahc.adapters.channel_control_ui import approval_text
from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.application.workspace_tools import WorkspaceTools
from ahc.cli.foreground_children import ForegroundChildren
from ahc.domain.workspace import WorkspaceIdentity


@pytest.mark.parametrize("variant", ["timeout", "missing", "expected", "foreign", "minion", "text", "valid", "long-timeout"])
def test_child_shape_rejected_before_approval_without_dispatch(variant):
    _, _, request = setup()
    request = replace(request, capabilities=("workspace.child",))
    workspace = SimpleNamespace(identity=WorkspaceIdentity(request.session, "work"), closed=False)
    children = ForegroundChildren(object(), object())
    children.children, children.workspace = object(), workspace
    tools = WorkspaceTools(workspace, children=children)
    approvals = []
    gate = SimpleNamespace(authorize=lambda *args: approvals.append(args) or False)
    bridge = CodexWorkspaceTools(request, tools, "thread", "turn", clock=lambda: 0, gate=gate)
    args = {"expected": workspace.identity.to_dict(), "child_id": "one",
            "task_text": "Mentally add 15+27", "timeout_ms": 600000}
    if variant == "timeout": args["timeout_ms"] = 0
    if variant == "long-timeout": args["timeout_ms"] = 3600000
    if variant == "missing": args.pop("timeout_ms")
    if variant == "expected": args.pop("expected")
    if variant == "foreign": args["expected"]["generation"] += 1
    if variant == "minion": args["execution_kind"] = "minion"
    if variant == "text": args["task_text"] = "x" * 4097
    call = message()
    call["params"].update(tool="child", arguments=args)
    response = bridge.respond(call)
    result = json.loads(response["result"]["contentItems"][0]["text"])
    assert response["result"]["success"] is False
    assert len(approvals) == (1 if variant in ("valid", "long-timeout") else 0)
    assert children.services == {}
    if variant not in ("valid", "long-timeout"):
        assert result["reason"] == "invalid-child-request"
        assert result["request_admitted"] is False
        assert result["unresolved_effects"] is False
    if variant == "expected":
        assert result["missing_fields"] == ["expected"]
        assert "complete" in result["hint"]
    if variant == "foreign":
        assert "missing_fields" not in result


def test_approval_card_displays_authoritative_expiry_before_long_detail(tmp_path):
    _, _, _, request, _ = approval_setup(tmp_path)
    text = approval_text(request)
    assert "核准截止：1970-01-01 00:01:40 UTC" in text
    assert text.index("核准截止") < text.index("/approve")
