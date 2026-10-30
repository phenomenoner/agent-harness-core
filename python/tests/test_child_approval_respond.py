import json

import pytest
from test_child_approval_store import state
from test_runtime_restore_cli import cli

from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_approval_queries import ChildApprovalQueries


@pytest.mark.parametrize("decision", ["approve-once", "deny"])
def test_operator_cli_responds_then_backend_consumes_once(tmp_path, decision):
    repo, _, _, request, approval, store, _ = state(tmp_path, realtime=True)
    store.publish(request, approval)
    args = (
        "runtime-child-approval-respond",
        "--runtime-root",
        repo.root,
        "--session-json",
        json.dumps(request.session.to_dict()),
        "--request-digest",
        approval.digest,
        "--decision",
        decision,
    )
    assert cli(*args)["outcome"] == decision
    cli(*args, code=2)
    assert store.consume(request.session, approval.digest).outcome == decision
    with pytest.raises(ContractError, match="resolved"):
        store.consume(request.session, approval.digest)


@pytest.mark.parametrize("change", ["cancel", "context", "policy", "expire"])
def test_respond_revalidates_after_query(tmp_path, change):
    repo, children, envelope, request, approval, store, now = state(tmp_path)
    store.publish(request, approval)
    operator = ChildApprovalQueries(repo, clock=lambda: now[0], monotonic=lambda: 0)
    assert operator.query(request.session, approval.digest)["current_claim"]
    if change == "cancel":
        children.cancel(envelope.parent, envelope.child_id)
    elif change == "context":
        repo._mutate(lambda c: c.execute("DELETE FROM runtime_child_approval_context"))
    elif change == "policy":
        repo._mutate(
            lambda c: c.execute("UPDATE runtime_effective_policies SET digest=?", ("f" * 64,))
        )
    else:
        now[0] = 96
        assert (
            operator.respond(request.session, approval.digest, "approve-once").outcome == "timeout"
        )
        return
    with pytest.raises(ContractError):
        operator.respond(request.session, approval.digest, "approve-once")
    with repo._read() as connection:
        row = connection.execute("SELECT state,outcome FROM runtime_child_approvals").fetchone()
        assert tuple(row) == ("pending", None)
