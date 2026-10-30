"""Controlled-peer approval translation; no vendor process or model request."""

from test_codex_effective_policy import Peer, policy, request

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.application.approval_port import UnavailableApproval
from ahc.domain.runtime_approval import ApprovalDecision


class DecisionPort:
    def __init__(self, outcome):
        self.outcome, self.requests = outcome, []

    def decide(self, approval, *, cancelled):
        self.requests.append(approval)
        return ApprovalDecision(approval.digest, self.outcome)


def approval_frame(identifier="approval", method="item/commandExecution/requestApproval"):
    return {
        "id": identifier,
        "method": method,
        "params": {"itemId": "item", "startedAtMs": 1, "threadId": "thread", "turnId": "turn"},
    }


def run_with_approval(
    port, *, outcome="approve-once", method="item/commandExecution/requestApproval"
):
    effective = policy()
    peer = Peer(effective)
    peer.messages.insert(3, approval_frame(method=method))
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        approval_port=port,
    )
    return peer, client.run_admitted(request(client, effective))


def test_matching_command_and_file_requests_dispatch_once_and_approve_once():
    for method in ("item/commandExecution/requestApproval", "item/fileChange/requestApproval"):
        port = DecisionPort("approve-once")
        peer, result = run_with_approval(port, method=method)
        assert result.disposition == "completed" and len(port.requests) == 1
        response = next(item for item in peer.sent if item.get("id") == "approval")
        assert response == {"id": "approval", "result": {"decision": "accept"}}
        assert "PRIVATE" not in str(port.requests[0].to_dict())


def test_deny_and_headless_manual_decline_without_silent_consent():
    peer, result = run_with_approval(DecisionPort("deny"))
    assert result.disposition == "completed"
    assert next(item for item in peer.sent if item.get("id") == "approval")["result"] == {
        "decision": "decline"
    }


def test_yolo_rejects_an_unexpected_prompt_without_dispatching_a_port():
    effective = policy(approval_mode="yolo")
    peer = Peer(effective)
    peer.messages.insert(3, approval_frame())
    port = DecisionPort("approve-once")
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        approval_port=port,
    )
    result = client.run_admitted(request(client, effective))
    assert result.disposition == "completed" and port.requests == []
    assert next(item for item in peer.sent if item.get("id") == "approval")["result"] == {
        "decision": "decline"
    }

    peer, result = run_with_approval(UnavailableApproval())
    assert result.disposition == "completed"
    assert next(item for item in peer.sent if item.get("id") == "approval")["result"] == {
        "decision": "decline"
    }


def test_late_and_duplicate_requests_cannot_approve():
    effective = policy(approval_wait_seconds=5)
    peer = Peer(effective)
    peer.messages.insert(3, approval_frame())
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        wall_clock=iter((10, 16)).__next__,
        effective_policy=effective,
        approval_port=DecisionPort("approve-once"),
    )
    result = client.run_admitted(request(client, effective))
    assert result.disposition == "completed"
    assert next(item for item in peer.sent if item.get("id") == "approval")["result"] == {
        "decision": "decline"
    }

    peer = Peer(effective)
    peer.messages[3:3] = [approval_frame(), approval_frame()]
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        approval_port=DecisionPort("approve-once"),
    )
    result = client.run_admitted(request(client, effective))
    assert result.disposition == "unknown"
    responses = [item for item in peer.sent if item.get("id") == "approval"]
    assert len(responses) == 2 and responses[0]["result"] == {"decision": "accept"}
    assert responses[1]["error"]["code"] == -32602

    peer = Peer(effective)
    frame = approval_frame()
    frame["params"]["turnId"] = "other"
    peer.messages.insert(3, frame)
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        approval_port=DecisionPort("approve-once"),
    )
    result = client.run_admitted(request(client, effective))
    assert result.disposition == "unknown"
    assert (
        next(item for item in peer.sent if item.get("id") == "approval")["error"]["code"] == -32602
    )
