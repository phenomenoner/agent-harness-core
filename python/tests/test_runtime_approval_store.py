import json
import subprocess
import sys
from dataclasses import replace

import pytest
from test_runtime_inbox import make

from ahc.domain.runtime_approval import ApprovalRequest
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict


def setup(tmp_path):
    repo, snapshot, original = make(tmp_path)
    policy = EffectivePolicy(1, "manual", "read-only", False, "a" * 64, 30)
    admitted = replace(original, effective_policy=policy)
    repo.begin(snapshot, admitted)
    approval = ApprovalRequest(
        admitted.session,
        admitted.operation_id,
        policy.digest,
        "thread",
        "turn",
        1,
        "b" * 64,
        100,
        "Read the selected workspace file",
    )
    clock = [90]
    store = RuntimeApprovalStore(repo, clock=lambda: clock[0])
    return repo, store, admitted, approval, clock


def test_policy_is_in_same_admission_and_changed_policy_cannot_reuse_operation(tmp_path):
    repo, _, admitted, _, _ = setup(tmp_path)
    with repo._read() as connection:
        row = connection.execute("SELECT * FROM runtime_effective_policies").fetchone()
        assert json.loads(row["payload_json"]) == admitted.effective_policy.to_dict()
        assert row["digest"] == admitted.effective_policy.digest
    changed = replace(
        admitted, effective_policy=replace(admitted.effective_policy, approval_mode="yolo")
    )
    assert request_fingerprint(changed) != request_fingerprint(admitted)
    with pytest.raises(ContractError, match="already bound"):
        repo.begin(repo.read_current(admitted.session.ref.virtual_session_id), changed)


def test_reopened_store_responds_and_consumes_once(tmp_path):
    repo, store, admitted, approval, _ = setup(tmp_path)
    assert store.publish(admitted, approval) == approval.digest
    assert store.publish(admitted, approval) == approval.digest
    reopened = RuntimeApprovalStore(RuntimeRepository(repo.root), clock=lambda: 91)
    assert reopened.query(admitted.session, approval.digest)["request"] == approval.to_dict()
    assert store.consume(admitted.session, approval.digest) is None
    reopened.respond(admitted.session, approval.digest, "approve-once")
    assert store.consume(admitted.session, approval.digest).outcome == "approve-once"
    with pytest.raises(ContractError, match="resolved"):
        reopened.consume(admitted.session, approval.digest)
    with pytest.raises(ContractError, match="resolved"):
        reopened.respond(admitted.session, approval.digest, "approve-once")
    with pytest.raises(ContractError):
        store.publish(admitted, approval)


def test_expired_response_and_cancel_do_not_approve(tmp_path):
    _, store, admitted, approval, clock = setup(tmp_path)
    store.publish(admitted, approval)
    store.respond(admitted.session, approval.digest, "approve-once")
    clock[0] = 100
    assert store.consume(admitted.session, approval.digest).outcome == "timeout"
    clock[0] = 90
    second = replace(approval, vendor_request_id=2)
    store.publish(admitted, second)
    assert store.consume(admitted.session, second.digest, cancelled=True).outcome == "cancelled"
    third = replace(approval, vendor_request_id=3)
    store.publish(admitted, third)
    clock[0] = 101
    assert store.respond(admitted.session, third.digest, "approve-once").outcome == "timeout"


def test_drift_stale_owner_or_reconciled_turn_cannot_approve(tmp_path):
    repo, store, admitted, approval, _ = setup(tmp_path)
    store.publish(admitted, approval)
    with pytest.raises(ContractError):
        store.publish(admitted, replace(approval, action_digest="c" * 64))
    with pytest.raises(ContractError):
        store.publish(admitted, replace(approval, policy_digest="c" * 64))
    with pytest.raises(ContractError):
        store.respond(
            replace(admitted.session, agent_id="foreign"), approval.digest, "approve-once"
        )
    repo.reconcile(admitted.session)
    with pytest.raises(WorkingSetConflict):
        store.respond(admitted.session, approval.digest, "approve-once")


def test_another_process_can_reply_while_execution_lease_is_held(tmp_path):
    repo, store, admitted, approval, _ = setup(tmp_path)
    store.publish(admitted, approval)
    script = """import json,sys
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
p=json.load(sys.stdin)
s=RuntimeApprovalStore(RuntimeRepository(p['root']),clock=lambda:91)
d=s.respond(VirtualSessionIdentity.from_dict(p['session']),p['digest'],'deny')
print(d.outcome)
"""
    with repo.execution_lock():
        result = subprocess.run(
            [sys.executable, "-c", script],
            input=json.dumps(
                {
                    "root": str(repo.root),
                    "session": admitted.session.to_dict(),
                    "digest": approval.digest,
                }
            ),
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        )
        assert result.stdout.strip() == "deny"
        assert store.consume(admitted.session, approval.digest).outcome == "deny"


def test_approval_requires_exact_live_admission_and_bounded_deadline(tmp_path):
    _, store, admitted, approval, _ = setup(tmp_path)
    for changed in (
        replace(admitted, effective_policy=None),
        replace(admitted, input_text="drift"),
    ):
        with pytest.raises(ContractError):
            store.publish(changed, approval)
    for expiry in (90, 121):
        with pytest.raises(ContractError):
            store.publish(admitted, replace(approval, expires_at=expiry))


def test_monotonic_wait_expires_even_if_wall_clock_does_not_advance(tmp_path):
    from ahc.application.pending_approval import PendingApprovalPort

    _, store, admitted, approval, _ = setup(tmp_path)
    monotonic = [0.0]

    def wait(_delay):
        monotonic[0] += 10

    port = PendingApprovalPort(
        store, admitted, clock=lambda: monotonic[0], wall_clock=lambda: 90, wait=wait
    )
    assert port.decide(approval, cancelled=lambda: False).outcome == "timeout"
    assert store.query(admitted.session, approval.digest)["state"] == "consumed"


def test_wait_has_no_open_transaction_and_cancel_does_not_replay(tmp_path):
    from ahc.application.pending_approval import PendingApprovalPort

    repo, store, admitted, approval, _ = setup(tmp_path)
    answered = []

    def respond(_delay):
        other = RuntimeApprovalStore(RuntimeRepository(repo.root), clock=lambda: 91)
        other.respond(admitted.session, approval.digest, "approve-once")
        answered.append(True)

    port = PendingApprovalPort(
        store, admitted, clock=lambda: 0, wall_clock=lambda: 90, wait=respond
    )
    assert port.decide(approval, cancelled=lambda: False).outcome == "approve-once"
    assert answered == [True]
    cancelled = replace(approval, vendor_request_id=2)
    assert port.decide(cancelled, cancelled=lambda: True).outcome == "cancelled"


def test_cli_query_and_response_with_execution_held(tmp_path, capsys):
    import time

    from ahc.cli.main import main

    repo, _, admitted, approval, _ = setup(tmp_path)
    store = RuntimeApprovalStore(repo)
    approval = replace(approval, expires_at=time.time() + 20)
    store.publish(admitted, approval)
    base = [
        "--runtime-root",
        str(repo.root),
        "--session-json",
        json.dumps(admitted.session.to_dict()),
    ]
    with repo.execution_lock():
        assert main(["runtime-approval-query", *base]) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["approvals"][0]["request_digest"] == approval.digest
        assert (
            main(
                [
                    "runtime-approval-respond",
                    *base,
                    "--request-digest",
                    approval.digest,
                    "--decision",
                    "approve-once",
                ]
            )
            == 0
        )
        result = json.loads(capsys.readouterr().out)
        assert result["outcome"] == "approve-once"
        assert store.consume(admitted.session, approval.digest).outcome == "approve-once"
        assert main(["runtime-approval-query", *base]) == 0
        assert json.loads(capsys.readouterr().out)["approvals"] == []


def test_policy_persistence_failure_rolls_back_entire_admission(tmp_path, monkeypatch):
    from ahc.infrastructure import runtime_repository

    repo, snapshot, original = make(tmp_path)
    admitted = replace(
        original, effective_policy=EffectivePolicy(1, "manual", "read-only", False, "a" * 64, 30)
    )

    def fail(_connection, _request):
        raise RuntimeError("controlled policy persistence failure")

    monkeypatch.setattr(runtime_repository, "persist_policy", fail)
    with pytest.raises(RuntimeError, match="controlled"):
        repo.begin(snapshot, admitted)
    assert repo.read_turn(admitted.session, admitted.operation_id) is None
    assert repo.read_current(admitted.session.ref.virtual_session_id) == snapshot
