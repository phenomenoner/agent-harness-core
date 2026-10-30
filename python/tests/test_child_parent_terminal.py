"""Parent terminal transitions retire local child ledger entries atomically."""

import sqlite3

import pytest
from test_runtime_core import setup as runtime_setup

from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.child_work import ChildParent, ChildWorkEnvelope
from ahc.domain.runtime_turn import request_fingerprint
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.working_repository import (
    WorkingRepositoryError,
    WorkingSetConflict,
    WorkingSetCorrupt,
)


def setup(tmp_path):
    repo, snapshot, request = runtime_setup(tmp_path)
    repo.begin(snapshot, request)
    parent = ChildParent(request.session, request.operation_id, request_fingerprint(request))
    return repo, request, parent, ChildWorkRepository(repo, clock=lambda: 100)


def admit(children, parent, child_id):
    return children.admit(ChildWorkEnvelope(child_id, parent, "work", "workspace", 200))


def states(children, parent):
    return {value["child_id"]: value["state"] for value in children.read(parent)}


def test_finish_retires_only_pending_children_in_parent_transaction(tmp_path):
    repo, request, parent, children = setup(tmp_path)
    admit(children, parent, "queued")
    admit(children, parent, "claimed")
    children.claim(parent, "claimed", "claim")
    admit(children, parent, "terminal")
    children.claim(parent, "terminal", "terminal-claim")
    terminal = children.complete(parent, "terminal", "terminal-claim", "child result")

    repo.finish(request, {"accepted": True, "output_text": "parent result"})

    assert states(children, parent) == {
        "queued": "cancelled",
        "claimed": "unknown",
        "terminal": "terminal",
    }
    assert children.read(parent)[2]["result"] == terminal["result"]
    assert repo.read_turn(request.session, request.operation_id).status == "terminal"


def test_cancel_then_reconcile_retires_and_fences_late_child_completion(tmp_path):
    repo, request, parent, children = setup(tmp_path)
    admit(children, parent, "claimed")
    children.claim(parent, "claimed", "claim")
    WorkingContinuityService(repo).cancel(repo.read_current("session"))
    assert states(children, parent) == {"claimed": "claimed"}

    reconciled = repo.reconcile(request.session)

    assert len(reconciled) == 1
    assert states(children, parent) == {"claimed": "unknown"}
    with pytest.raises(WorkingSetConflict):
        children.complete(parent, "claimed", "claim", "late")


def test_parent_finish_precommit_failure_rolls_back_child_retirement(tmp_path):
    repo, request, parent, children = setup(tmp_path)
    admit(children, parent, "queued")
    admit(children, parent, "claimed")
    children.claim(parent, "claimed", "claim")

    with pytest.raises(WorkingRepositoryError, match="injected"):
        repo.finish(
            request, {"accepted": True, "output_text": "parent result"}, fail_before_commit=True
        )

    assert states(children, parent) == {"queued": "queued", "claimed": "claimed"}
    assert repo.read_turn(request.session, request.operation_id).status == "submitted"


def test_corrupt_child_record_aborts_parent_and_preserves_all_child_rows(tmp_path):
    repo, request, parent, children = setup(tmp_path)
    admit(children, parent, "queued")
    admit(children, parent, "claimed")
    children.claim(parent, "claimed", "claim")
    repo._mutate(
        lambda connection: connection.execute(
            "UPDATE runtime_children SET envelope_json='{}' WHERE child_id='claimed'"
        )
    )
    with repo._read() as connection:
        before = tuple(
            connection.execute(
                "SELECT child_id,state,claim_token,envelope_json,result_json "
                "FROM runtime_children ORDER BY child_id"
            )
        )

    with pytest.raises(WorkingSetCorrupt):
        repo.finish(request, {"accepted": True, "output_text": "parent result"})

    with repo._read() as connection:
        after = tuple(
            connection.execute(
                "SELECT child_id,state,claim_token,envelope_json,result_json "
                "FROM runtime_children ORDER BY child_id"
            )
        )
    assert after == before
    assert repo.read_turn(request.session, request.operation_id).status == "submitted"


def test_retire_parent_accepts_an_older_store_without_child_table():
    connection = sqlite3.connect(":memory:")
    try:
        assert ChildWorkRepository.retire_parent(connection, "parent") == ()
    finally:
        connection.close()
