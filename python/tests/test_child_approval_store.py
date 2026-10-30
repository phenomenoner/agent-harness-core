import time
from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.application.pending_approval import PendingApprovalPort
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.runtime_approval import ApprovalRequest
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_approval_store import ChildApprovalStore
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository


def state(tmp_path, *, realtime=False):
    repo, snapshot, original = setup(tmp_path)
    policy = EffectivePolicy(1, "manual", "read-only", False, "a" * 64, 30)
    parent = replace(original, effective_policy=policy,
                     deadline=time.monotonic() + 60 if realtime else 100)
    repo.begin(snapshot, parent)
    now = [time.time() if realtime else 90]
    children = ChildDispatchRepository(repo, parent, clock=lambda: int(now[0] * 1000),
                                       monotonic_clock=time.monotonic if realtime else lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "task", parent.workspace_scope,
                                 int((now[0] + 10) * 1000))
    children.admit(envelope)
    children.claim(envelope.parent, envelope.child_id, "token")
    request = replace(parent, operation_id="child", input_text="task", capabilities=())
    approval = ApprovalRequest(parent.session, "child", policy.digest, "thread", "turn", 1,
                               "b" * 64, now[0] + 5, "Read selected file")
    store = ChildApprovalStore(children, envelope, "token", request, clock=lambda: now[0])
    return repo, children, envelope, request, approval, store, now


@pytest.mark.parametrize("outcome", ["approve-once", "deny", "timeout", "cancelled"])
def test_child_approval_is_consumed_once_and_not_parent_approval(tmp_path, outcome):
    repo, children, envelope, request, approval, store, now = state(tmp_path)
    assert store.publish(request, approval) == approval.digest
    assert store.publish(request, approval) == approval.digest
    reopened = ChildApprovalStore(children, envelope, "token", request, clock=lambda: now[0])
    assert reopened.consume(request.session, approval.digest) is None
    if outcome in ("approve-once", "deny"):
        reopened.respond(request.session, approval.digest, outcome)
    elif outcome == "timeout":
        now[0] = 96
    result = store.consume(request.session, approval.digest, cancelled=outcome == "cancelled")
    assert result.outcome == outcome
    with pytest.raises(ContractError, match="resolved"):
        reopened.consume(request.session, approval.digest)
    assert repo.read_turn(request.session, envelope.child_id) is None


def test_changed_claim_or_vendor_action_cannot_reuse_approval(tmp_path):
    _, children, envelope, request, approval, store, _ = state(tmp_path)
    store.publish(request, approval)
    with pytest.raises(ContractError, match="slot"):
        store.publish(request, replace(approval, action_digest="c" * 64))
    with pytest.raises(ContractError, match="mismatch"):
        store.publish(request, replace(approval, operation_id=envelope.parent.operation_id))
    other = ChildApprovalStore(children, envelope, "other", request)
    with pytest.raises(ContractError, match="claim"):
        other.respond(request.session, approval.digest, "approve-once")
    children.cancel(envelope.parent, envelope.child_id)
    with pytest.raises(ContractError, match="claim"):
        store.respond(request.session, approval.digest, "approve-once")


def test_pending_port_uses_child_decision_store(tmp_path):
    _, _, _, request, approval, store, now = state(tmp_path)
    waits = []

    def wait(seconds):
        waits.append(seconds)
        store.respond(request.session, approval.digest, "approve-once")

    port = PendingApprovalPort(store, request, clock=lambda: 0,
                               wall_clock=lambda: now[0], wait=wait)
    assert port.decide(approval, cancelled=lambda: False).outcome == "approve-once"
    assert len(waits) == 1


def test_pending_row_cannot_contain_an_unanswered_approval(tmp_path):
    repo, _, _, request, approval, store, _ = state(tmp_path)
    store.publish(request, approval)
    repo._mutate(lambda connection: connection.execute(
        "UPDATE runtime_child_approvals SET outcome='approve-once'"))
    with pytest.raises(ContractError, match="record invalid"):
        store.consume(request.session, approval.digest)
