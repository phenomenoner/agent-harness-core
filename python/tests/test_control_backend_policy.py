"""Bounded policy and wire checks without a Codex process."""

import copy

import pytest
from test_codex_approval import DecisionPort, approval_frame
from test_codex_effective_policy import Peer, policy, request
from test_foreground_config_v2 import valid_config

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.sessions import ContractError


def _run(policy_value, *, approval_port=None, service_tier=None):
    peer = Peer(policy_value)
    if service_tier is not None:
        peer.messages[1]["result"]["serviceTier"] = service_tier
    if policy_value.codex_approvals_reviewer is not None:
        peer.messages[1]["result"]["approvalsReviewer"] = policy_value.codex_approvals_reviewer
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=policy_value,
        approval_port=approval_port,
        service_tier=service_tier,
    )
    return peer, client, client.run_admitted(request(client, policy_value))


def test_legacy_manual_and_yolo_mappings_preserve_sandbox_and_reviewer_omission():
    manual, yolo = policy(), policy(approval_mode="yolo")
    assert manual.codex_approval_policy == "on-request"
    assert yolo.codex_approval_policy == "never"
    assert manual.sandbox_mode == yolo.sandbox_mode == "workspace-write"
    assert manual.codex_approvals_reviewer is yolo.codex_approvals_reviewer is None


def test_auto_approve_requests_native_reviewer_and_unexpected_callback_uses_port():
    effective = policy(approval_mode="auto-approve")
    port = DecisionPort("deny")
    peer, client, result = _run(effective, approval_port=port)
    assert result.disposition == "completed"
    for method in ("thread/start", "turn/start"):
        params = next(item["params"] for item in peer.sent if item.get("method") == method)
        assert params["approvalPolicy"] == "on-request"
        assert params["approvalsReviewer"] == "auto_review"

    unverified = Peer(effective)
    client = CodexProtocolClient(
        unverified,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
    )
    assert client.run_admitted(request(client, effective)).disposition == "not-submitted"
    assert not any(item.get("method") == "turn/start" for item in unverified.sent)

    peer = Peer(effective)
    peer.messages[1]["result"]["approvalsReviewer"] = "auto_review"
    peer.messages.insert(3, approval_frame())
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        approval_port=port,
    )
    result = client.run_admitted(request(client, effective))
    assert result.disposition == "completed" and len(port.requests) == 1
    assert next(item for item in peer.sent if item.get("id") == "approval")["result"] == {
        "decision": "decline"
    }


def test_always_approve_callback_accepts_without_prompting_port():
    effective = policy(approval_mode="always-approve")
    port = DecisionPort("deny")
    peer = Peer(effective)
    peer.messages.insert(3, approval_frame())
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
        "decision": "accept"
    }


def test_priority_service_tier_is_optional_digest_bound_and_strictly_configured():
    baseline = parse_foreground_config_v2(valid_config())
    payload = copy.deepcopy(valid_config())
    payload["backend"]["service_tier"] = "priority"
    priority = parse_foreground_config_v2(payload)
    assert "service_tier" not in baseline.backend
    assert priority.backend["service_tier"] == "priority"
    assert priority.configuration_digest != baseline.configuration_digest

    peer, client, result = _run(policy(), service_tier="priority")
    assert result.disposition == "completed"
    assert client.configuration_digest != CodexProtocolClient(
        Peer(policy()), cwd="workspace", model="fixture", effective_policy=policy()
    ).configuration_digest
    for method in ("thread/start", "turn/start"):
        assert next(item["params"] for item in peer.sent if item.get("method") == method)[
            "serviceTier"
        ] == "priority"

    payload["backend"]["service_tier"] = "normal"
    with pytest.raises(ContractError, match="service_tier"):
        parse_foreground_config_v2(payload)
