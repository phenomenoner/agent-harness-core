import json

import pytest
from test_codex_approval import DecisionPort, approval_frame
from test_codex_effective_policy import Peer, policy, request

from ahc.adapters.codex_approval_display import approval_summary
from ahc.adapters.codex_client import CodexProtocolClient
from ahc.domain.sessions import ContractError


def test_display_keeps_command_cwd_and_permission_extensions_as_inert_data():
    params = {
        "command": "printf '\\n'; echo \u001b[2J\u202e",
        "cwd": "/home/ahc/work",
        "reason": "Generate report",
        "additionalPermissions": {"network": True},
        "proposedExecpolicyAmendment": ["python", "report.py"],
        "threadId": "thread",
    }
    text = approval_summary("item/commandExecution/requestApproval", params)
    assert all(ord(character) >= 32 and ord(character) < 127 for character in text)
    details = json.loads(text)["details"]
    assert details == {key: value for key, value in params.items() if key != "threadId"}


def test_file_scope_and_missing_command_are_explicit_and_oversize_never_truncated():
    file = json.loads(approval_summary("item/fileChange/requestApproval", {"grantRoot": "/work"}))
    assert file["details"]["grantRoot"] == "/work" and "diff" in file["scope_note"]
    command = json.loads(approval_summary("item/commandExecution/requestApproval", {}))
    assert "command text" in command["scope_note"]
    with pytest.raises(ContractError, match="display bound"):
        approval_summary("item/commandExecution/requestApproval", {"command": "x" * 4096})


def test_vendor_action_details_reach_the_correlated_decision_port():
    effective = policy()
    peer = Peer(effective)
    frame = approval_frame()
    frame["params"].update(command="python report.py", cwd="/home/ahc/work", reason="Build report")
    peer.messages.insert(3, frame)
    port = DecisionPort("approve-once")
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        approval_port=port,
    )
    assert client.run_admitted(request(client, effective)).disposition == "completed"
    assert json.loads(port.requests[0].action_summary)["details"]["command"] == "python report.py"
    assert (
        next(item for item in peer.sent if item.get("id") == "approval")["result"]["decision"]
        == "accept"
    )
