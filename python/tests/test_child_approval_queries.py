import json

from test_child_approval_store import state
from test_runtime_restore_cli import cli

from ahc.infrastructure.child_approval_queries import ChildApprovalQueries
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_new_operator_context_queries_claim_and_history(tmp_path):
    repo, children, envelope, request, approval, store, now = state(tmp_path)
    store.publish(request, approval)
    queries = ChildApprovalQueries(RuntimeRepository(repo.root), clock=lambda: now[0],
                                    monotonic=lambda: 0)
    result = queries.query(request.session, approval.digest)
    assert result["current_claim"] and result["parent_operation_id"] == envelope.parent.operation_id
    assert queries.pending(request.session) == (result,)
    output = cli("runtime-child-approval-query", "--runtime-root", repo.root,
        "--session-json", json.dumps(request.session.to_dict()),
        "--request-digest", approval.digest)
    assert output["approvals"][0]["request"] == approval.to_dict()
    assert "token" not in json.dumps(output)
    children.cancel(envelope.parent, envelope.child_id)
    assert not queries.query(request.session, approval.digest)["current_claim"]


def test_old_or_changed_context_never_looks_current(tmp_path):
    repo, _, _, request, approval, store, now = state(tmp_path)
    store.publish(request, approval)
    query = ChildApprovalQueries(repo, clock=lambda: now[0], monotonic=lambda: 0)
    repo._mutate(lambda c: c.execute("UPDATE runtime_child_approval_context SET deadline='101'"))
    assert not query.query(request.session, approval.digest)["current_claim"]
    repo._mutate(lambda c: c.execute("DELETE FROM runtime_child_approval_context"))
    assert not query.query(request.session, approval.digest)["current_claim"]
