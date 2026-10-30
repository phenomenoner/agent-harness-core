from dataclasses import replace

import pytest

from ahc.application.approval_port import UnavailableApproval
from ahc.domain.runtime_approval import ApprovalDecision, ApprovalRequest
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def request(**changes):
    return replace(
        ApprovalRequest(
            VirtualSessionIdentity.initial("session", "agent", "route"),
            "operation",
            "a" * 64,
            "thread",
            "turn",
            1,
            "b" * 64,
            100,
            "Run the selected local command",
        ),
        **changes,
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"operation_id": "other"},
        {"backend_thread": "other"},
        {"backend_turn": "other"},
        {"vendor_request_id": "1"},
        {"policy_digest": "c" * 64},
        {"action_digest": "c" * 64},
        {"expires_at": 101},
        {"action_summary": "A different displayed action"},
        {"session": VirtualSessionIdentity.initial("other", "agent", "route")},
    ],
)
def test_decision_cannot_approve_another_binding(changes):
    decision = ApprovalDecision(request().digest, "approve-once")
    with pytest.raises(ContractError, match="correlation"):
        decision.resolve(request(**changes), now=90)


def test_late_or_cancelled_approval_does_not_authorize():
    pending = request()
    decision = ApprovalDecision(pending.digest, "approve-once")
    assert decision.resolve(pending, now=99) == "approve-once"
    assert decision.resolve(pending, now=100) == "timeout"
    assert decision.resolve(pending, now=99, cancelled=True) == "cancelled"


def test_headless_manual_returns_unavailable_without_waiting():
    pending = request()
    assert UnavailableApproval().decide(pending, cancelled=lambda: False).outcome == "unavailable"
    assert UnavailableApproval().decide(pending, cancelled=lambda: True).outcome == "cancelled"


@pytest.mark.parametrize("identity", [True, None, [], 2**53, "", "secret\nline"])
def test_invalid_vendor_identity_rejects(identity):
    with pytest.raises(ContractError):
        request(vendor_request_id=identity)
