from dataclasses import replace

import pytest
from test_runtime_core import setup as runtime_setup

from ahc.domain.child_work import ChildParent, ChildWorkEnvelope
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict


def setup(tmp_path, *, now=100):
    repo, snapshot, request = runtime_setup(tmp_path)
    repo.begin(snapshot, request)
    parent = ChildParent(request.session, request.operation_id, request_fingerprint(request))
    clock = [now]
    return repo, parent, clock, ChildWorkRepository(repo, clock=lambda: clock[0])


def envelope(parent, *, child_id="child", deadline=200, text="work"):
    return ChildWorkEnvelope(child_id, parent, text, "workspace", deadline)


def test_duplicate_admission_is_idempotent_and_conflict_is_rejected(tmp_path):
    _, parent, _, children = setup(tmp_path)
    first = children.admit(envelope(parent))
    assert children.admit(envelope(parent)) == first
    with pytest.raises(ContractError, match="already bound"):
        children.admit(envelope(parent, text="different"))


def test_stale_or_wrong_parent_rejects_before_child_mutation(tmp_path):
    repo, parent, _, children = setup(tmp_path)
    children.admit(envelope(parent))
    foreign = ChildParent(
        replace(parent.session, agent_id="foreign"), parent.operation_id, parent.request_digest
    )
    with pytest.raises(WorkingSetConflict):
        children.claim(foreign, "child", "token")
    assert children.read(parent)[0]["state"] == "queued"
    from ahc.application.working_continuity import WorkingContinuityService

    WorkingContinuityService(repo).cancel(repo.read_current("session"))
    with pytest.raises(WorkingSetConflict):
        children.cancel(parent, "child")


def test_cancel_deadline_and_late_completion_are_terminal_without_replay(tmp_path):
    _, parent, clock, children = setup(tmp_path)
    children.admit(envelope(parent, child_id="cancel"))
    assert children.cancel(parent, "cancel")["state"] == "cancelled"
    assert children.claim(parent, "cancel", "token")["state"] == "cancelled"
    children.admit(envelope(parent, child_id="expired", deadline=101))
    clock[0] = 101
    assert children.claim(parent, "expired", "token")["state"] == "cancelled"
    clock[0] = 100
    children.admit(envelope(parent, child_id="late", deadline=101))
    assert children.claim(parent, "late", "token")["state"] == "claimed"
    clock[0] = 101
    assert children.complete(parent, "late", "token", "answer")["state"] == "unknown"


def test_claim_reopen_reconcile_marks_unknown_without_dispatch_or_retry(tmp_path):
    repo, parent, _, children = setup(tmp_path)
    children.admit(envelope(parent))
    assert children.claim(parent, "child", "token")["state"] == "claimed"
    reopened = ChildWorkRepository(RuntimeRepository(repo.root), clock=lambda: 100)
    assert reopened.reconcile(parent)[0]["state"] == "unknown"
    assert reopened.claim(parent, "child", "token")["state"] == "unknown"


def test_same_token_does_not_issue_a_second_claim(tmp_path):
    _, parent, _, children = setup(tmp_path)
    children.admit(envelope(parent))
    children.claim(parent, "child", "token")
    with pytest.raises(ContractError, match="already claimed"):
        children.claim(parent, "child", "token")


def test_terminal_completion_cannot_replace_bytes_or_claim_token(tmp_path):
    _, parent, _, children = setup(tmp_path)
    children.admit(envelope(parent))
    children.claim(parent, "child", "token")
    terminal = children.complete(parent, "child", "token", "first")
    assert children.complete(parent, "child", "token", "first") == terminal
    with pytest.raises(ContractError):
        children.complete(parent, "child", "other", "first")
    with pytest.raises(ContractError):
        children.complete(parent, "child", "token", "replacement")
    assert children.mailbox(parent) == (terminal,)


def test_reconcile_does_not_erase_corrupt_claim_evidence(tmp_path):
    from ahc.infrastructure.working_repository import WorkingSetCorrupt

    repo, parent, _, children = setup(tmp_path)
    children.admit(envelope(parent))
    children.claim(parent, "child", "token")
    repo._mutate(
        lambda connection: connection.execute(
            "UPDATE runtime_children SET claim_token='invalid token' WHERE child_id='child'"
        )
    )
    with pytest.raises(WorkingSetCorrupt):
        children.reconcile(parent)
    with repo._read() as connection:
        row = connection.execute("SELECT state,claim_token FROM runtime_children").fetchone()
        assert tuple(row) == ("claimed", "invalid token")


def test_terminal_mailbox_is_parent_evidence_not_a_runtime_final_intent(tmp_path):
    repo, parent, _, children = setup(tmp_path)
    children.admit(envelope(parent))
    children.claim(parent, "child", "token")
    terminal = children.complete(parent, "child", "token", "untrusted child output")
    assert terminal["state"] == "terminal"
    assert children.mailbox(parent) == (terminal,)
    assert repo.intents(parent.session) == ()


def test_invalid_completion_cannot_stringify_or_mutate_claimed_child(tmp_path):
    _, parent, _, children = setup(tmp_path)
    children.admit(envelope(parent))
    children.claim(parent, "child", "token")

    class Poison:
        def __str__(self):
            raise AssertionError("must not stringify child output")

    with pytest.raises(ContractError, match="another token"):
        children.complete(parent, "child", "other", "answer")
    with pytest.raises(ContractError):
        children.complete(parent, "child", "token", Poison())
    assert children.read(parent)[0]["state"] == "claimed"
