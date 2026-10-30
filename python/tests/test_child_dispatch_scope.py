"""Borrowing the parent lease does not make execution locks generally reentrant."""

from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.domain.child_work import ChildParent
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_callback_failure_after_port_close_is_unknown_without_replay(tmp_path):
    from test_child_dispatch import state

    from ahc.application.child_dispatch import ChildDispatchService

    _, _, _, children, envelope = state(tmp_path)
    closed = []

    class Port:
        def run(self, *_args, **_kwargs):
            return "answer"

        def close(self):
            closed.append(True)

    def cancelled():
        if closed:
            raise RuntimeError("private callback failure")
        return False

    service = ChildDispatchService(children, lambda _: Port())
    result = service.dispatch(envelope, "token", cancelled=cancelled)
    assert result["state"] == "unknown" and result["result"] is None
    assert service.dispatch(envelope, "other")["state"] == "unknown"
    assert closed == [True]


def test_only_exact_parent_turn_can_borrow_held_execution_scope(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    repo.begin(snapshot, request)
    children = ChildDispatchRepository(repo, request)
    with repo.execution_lock(), repo.workspace_turn_scope(request):
        with children.execution_scope(children.parent):
            pass
        with pytest.raises(WorkingWriterBusy):
            with repo.execution_lock():
                pytest.fail("ordinary execution lock became reentrant")
        other_request = replace(request, operation_id="other")
        other = ChildDispatchRepository(repo, other_request)
        with pytest.raises(ContractError):
            with other.execution_scope(other.parent):
                pytest.fail("foreign parent borrowed execution grant")
        foreign = ChildParent(request.session, "other", request_fingerprint(request))
        with pytest.raises(ContractError):
            with children.execution_scope(foreign):
                pytest.fail("foreign parent passed request binding")
