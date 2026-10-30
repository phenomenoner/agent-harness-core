import time
from contextvars import copy_context
from threading import Thread

import pytest
from test_backend_custody import Runner, ready_to_create

from ahc.application.custodied_backend import CustodiedBackend
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_durable_turn_composes_custody_create_and_recover(tmp_path):
    repo, custody, lease, workspace, unit = ready_to_create(tmp_path)
    snapshot = repo.read_current(workspace.session.ref.virtual_session_id)
    request = AgentTurnRequest(
        workspace.session,
        "op",
        snapshot.goal.digest,
        "input",
        (),
        "a" * 64,
        "b" * 64,
        "work",
        time.monotonic() + 30,
    )

    class Creator(Runner):
        def create(self, *args, **kwargs):
            self.calls.append("create")
            return unit

    runner = Creator(unit)

    class Backend:
        def invoke(self, bound):
            assert (
                custody.create(lease, workspace, runner, ["/bin/sleep", "1"], lifetime_seconds=1)
                == unit
            )
            assert custody.recover(lease, runner) == (unit,)
            return AgentTurnObservation(
                bound, "completed", "fixture", 1, 1, "thread", "turn", "done"
            )

    try:
        result = RuntimeService(repo, Backend()).execute(snapshot, request)
        assert result.result["accepted"]
        assert runner.calls == ["create", "observe", "stop", "observe"]
        assert RuntimeService(repo, Backend()).execute(snapshot, request) == result
        assert runner.calls == ["create", "observe", "stop", "observe"]
    finally:
        lease.close()


def test_backend_scope_does_not_make_ordinary_lock_reentrant(tmp_path):
    repo, _, lease, workspace, _ = ready_to_create(tmp_path)
    try:
        with repo.execution_lock():
            with pytest.raises(WorkingWriterBusy):
                with repo.backend_execution_lock(workspace.session, "work"):
                    pytest.fail("ungranted nesting")
    finally:
        lease.close()


def test_custodied_backend_client_failure_keeps_identity_for_cleanup(tmp_path):
    repo, custody, lease, workspace, unit = ready_to_create(tmp_path)
    lease.close()
    snapshot = repo.read_current(workspace.session.ref.virtual_session_id)
    request = AgentTurnRequest(
        workspace.session,
        "op",
        snapshot.goal.digest,
        "input",
        (),
        "a" * 64,
        "b" * 64,
        "work",
        time.monotonic() + 30,
    )

    class Creator(Runner):
        def create(self, *args, **kwargs):
            self.calls.append("create")
            return unit

    runner = Creator(unit)

    def failed_client(identity):
        assert identity == unit
        raise RuntimeError("client setup failure")

    backend = CustodiedBackend(
        repo, workspace, runner, ["/bin/sleep", "1"], 1, failed_client, custody=custody
    )
    assert backend.custody is custody
    result = RuntimeService(repo, backend).execute(snapshot, request)
    assert result.result["disposition"] == "unknown" and backend.lease is not None
    backend.close()
    assert backend.closed and backend.lease is None
    assert runner.calls == ["create", "observe", "stop", "observe"]
    backend.close()
    with pytest.raises(ContractError, match="replayed"):
        backend.invoke(request)


def test_turn_grant_rejects_foreign_scope_store_and_copied_thread(tmp_path):
    repo, _, lease, workspace, _ = ready_to_create(tmp_path)
    snapshot = repo.read_current(workspace.session.ref.virtual_session_id)
    request = AgentTurnRequest(
        workspace.session,
        "op",
        snapshot.goal.digest,
        "input",
        (),
        "a" * 64,
        "b" * 64,
        "work",
        time.monotonic() + 30,
    )
    other = RuntimeRepository(repo.root)
    failures = []
    try:
        with repo.execution_lock():
            repo.begin(snapshot, request)
            with repo.workspace_turn_scope(request):
                for store, name in [(repo, "other"), (other, "work")]:
                    with pytest.raises(ContractError):
                        with store.backend_execution_lock(workspace.session, name):
                            pytest.fail("foreign scope")

                def attempt():
                    try:
                        with repo.backend_execution_lock(workspace.session, "work"):
                            failures.append("accepted")
                    except ContractError:
                        failures.append("rejected")

                context = copy_context()
                thread = Thread(target=lambda: context.run(attempt))
                thread.start()
                thread.join(5)
                assert not thread.is_alive() and failures == ["rejected"]
    finally:
        lease.close()
