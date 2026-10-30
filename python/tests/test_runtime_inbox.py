from dataclasses import replace

import pytest
from skill_test_support import goal
from test_codex_client import setup

from ahc.domain.sessions import ContractError
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def make(tmp_path):
    _, _, request = setup()
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = repo.create(WorkingSetSnapshot.initial(request.session, goal()))
    return repo, snapshot, replace(request, goal_digest=snapshot.goal.digest)


def test_submit_reopen_claim_and_query_share_durable_admission(tmp_path):
    repo, snapshot, request = make(tmp_path)
    inbox = RuntimeInbox(repo)
    assert inbox.query(request.session, "op") is None
    assert inbox.next_pending(request.session) is None
    receipt = inbox.submit(
        request.session, request.operation_id, request.goal_digest, request.input_text
    )
    assert (
        inbox.submit(request.session, request.operation_id, request.goal_digest, request.input_text)
        == receipt
    )
    reopened = RuntimeInbox(RuntimeRepository(repo.root))
    assert reopened.next_pending(request.session)["input_text"] == request.input_text
    assert reopened.query(request.session, request.operation_id)["status"] == "pending"
    repo.begin(snapshot, request)
    assert reopened.next_pending(request.session) is None
    assert reopened.query(request.session, request.operation_id)["status"] == "submitted"
    repo.reconcile(request.session)
    assert reopened.query(request.session, request.operation_id)["status"] == "terminal"
    assert reopened.next_pending(request.session) is None


def test_conflicting_submission_and_direct_admission_cannot_steal_operation(tmp_path):
    repo, snapshot, request = make(tmp_path)
    inbox = RuntimeInbox(repo)
    inbox.submit(request.session, request.operation_id, request.goal_digest, request.input_text)
    with pytest.raises(ContractError):
        inbox.submit(request.session, request.operation_id, request.goal_digest, "different")
    with pytest.raises(ContractError):
        repo.begin(snapshot, replace(request, input_text="different"))
    assert repo.read_turn(request.session, request.operation_id) is None


def test_pending_limit_and_query_owner_boundary(tmp_path):
    repo, _, request = make(tmp_path)
    inbox = RuntimeInbox(repo)
    for index in range(64):
        inbox.submit(request.session, f"op-{index}", request.goal_digest, "input")
    with pytest.raises(ContractError):
        inbox.submit(request.session, "overflow", request.goal_digest, "input")
    with pytest.raises(ContractError):
        inbox.query(replace(request.session, agent_id="foreign"), "op-0")


def test_cli_submit_query_do_not_admit_backend(tmp_path):
    import json

    from ahc.cli.main import build_parser
    from ahc.cli.runtime import run_runtime_command

    repo, _, request = make(tmp_path)
    base = [
        "--runtime-root",
        str(repo.root),
        "--session-json",
        json.dumps(request.session.to_dict()),
        "--operation-id",
        request.operation_id,
    ]
    submit = build_parser().parse_args(
        [
            "runtime-submit",
            *base,
            "--input-text",
            request.input_text,
            "--goal-digest",
            request.goal_digest,
        ]
    )
    assert run_runtime_command(submit)[0] == 0
    query = build_parser().parse_args(["runtime-query", *base])
    code, result = run_runtime_command(query)
    assert code == 0 and result["submission"]["status"] == "pending"
    assert repo.read_turn(request.session, request.operation_id) is None
