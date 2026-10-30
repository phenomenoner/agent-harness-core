"""Evaluation borrowing cannot widen or outlive the admitted parent lease."""

import time
from contextvars import copy_context
from threading import Thread
from types import SimpleNamespace

import pytest
from skill_test_support import goal, session

from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.sessions import ContractError
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_execution_scope import (
    evaluation_workspace,
    permits_evaluation_workspace,
)
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy

NAME = "eval-" + "e" * 64


def setup(tmp_path, *, remaining=30):
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    current = store.create(WorkingSetSnapshot.initial(session(1), goal()))
    request = AgentTurnRequest(current.session, "parent-op", current.goal.digest,
        "input", (), "a" * 64, "b" * 64, "parent", time.monotonic() + remaining)
    return store, current, request


def test_exact_workspace_borrow_restores_parent_and_revokes_copied_scope(tmp_path):
    store, current, request = setup(tmp_path)
    closed = []
    with store.execution_lock():
        store.begin(current, request)
        with store.workspace_turn_scope(request):
            with evaluation_workspace(store, current.session, NAME) as remaining:
                assert 0 < remaining <= 30
                fresh = store.read_current(current.session.ref.virtual_session_id)
                with RuntimeWorkspace.open(store, fresh, NAME,
                        lambda identity, checkpoint: SimpleNamespace(
                            identity=identity, close=lambda: closed.append(True))) as workspace:
                    with store.workspace_execution_lock(workspace.identity):
                        with store.backend_execution_lock(current.session, NAME,
                                                          resource_role="python-workspace"):
                            pass
                    for name, role in [(NAME, "backend"), (NAME + "0", "python-workspace")]:
                        with pytest.raises(ContractError):
                            with store.backend_execution_lock(current.session, name,
                                                              resource_role=role):
                                pytest.fail("foreign capability borrowed")
                    with pytest.raises(WorkingWriterBusy):
                        with store.execution_lock():
                            pytest.fail("ordinary lock became reentrant")
                    copied = copy_context()
                    failures = []

                    def other_thread():
                        with pytest.raises(ContractError):
                            permits_evaluation_workspace(store, current.session, NAME)
                        failures.append("rejected")

                    thread = Thread(target=lambda: copied.run(other_thread))
                    thread.start()
                    thread.join(5)
                    assert not thread.is_alive() and failures == ["rejected"]
                    with pytest.raises(ContractError):
                        with evaluation_workspace(store, current.session, NAME):
                            pytest.fail("recursive evaluation")
            assert closed == [True]
            with pytest.raises(ContractError):
                copied.run(permits_evaluation_workspace, store, current.session, NAME)
            with store.backend_execution_lock(current.session, "parent"):
                pass
            with pytest.raises(ContractError):
                with store.backend_execution_lock(current.session, NAME,
                                                  resource_role="python-workspace"):
                    pytest.fail("expired borrow")


@pytest.mark.parametrize("failure", ["deadline", "foreign-store", "foreign-session",
                                     "parent-name", "stale-current", "not-submitted"])
def test_evaluation_refuses_ineligible_parent_before_workspace_allocation(tmp_path, failure):
    store, current, request = setup(tmp_path, remaining=-1 if failure == "deadline" else 30)
    with store.execution_lock():
        store.begin(current, request)
        with store.workspace_turn_scope(request):
            target = RuntimeRepository(store.root) if failure == "foreign-store" else store
            owner = session(2) if failure == "foreign-session" else current.session
            name = "parent" if failure == "parent-name" else NAME
            if failure in ("stale-current", "not-submitted"):
                # Simulate a changed durable parent beneath an otherwise live scope.
                if failure == "stale-current":
                    fresh = store.read_current(current.session.ref.virtual_session_id)
                    store._mutate(lambda db: store._advance(
                        db, fresh, fresh.revise(active_turn_id=None)))
                else:
                    store._mutate(lambda db: db.execute(
                        "UPDATE runtime_turns SET status='terminal' WHERE operation_id=?",
                        (request.operation_id,)))
            with pytest.raises(ContractError):
                with evaluation_workspace(target, owner, name):
                    pytest.fail("ineligible parent borrowed")
            with store._read() as db:
                assert db.execute("SELECT count(*) FROM workspace_heads").fetchone()[0] == 0
