"""Real SQLite lifecycle/restart seams with zero provider execution."""

from dataclasses import replace

import pytest
from skill_test_support import goal

from ahc.application.runtime_service import RuntimeService
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingRepositoryError, WorkingSetConflict


class Backend:
    calls = 0

    def invoke(self, request):
        self.calls += 1
        return AgentTurnObservation(
            request, "completed", "fixture", 1, 1, "thread", "turn", "answer"
        )


def setup(tmp_path):
    repository = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("session", "agent", "route"),
        goal(),
    )
    repository.create(snapshot)
    request = AgentTurnRequest(
        snapshot.session,
        "operation",
        snapshot.goal.digest,
        "question",
        (),
        "a" * 64,
        "b" * 64,
        "workspace",
        100,
    )
    return repository, snapshot, request


def test_result_intent_and_working_state_survive_reopen_without_replay(tmp_path):
    repo, snapshot, req = setup(tmp_path)
    backend = Backend()
    result = RuntimeService(repo, backend).execute(snapshot, req)
    assert result.result["accepted"] is True
    assert backend.calls == 1
    reopened = RuntimeRepository(repo.root)
    current = reopened.read_current("session")
    assert current.status == "active"  # Turn completion does not complete the goal.
    assert current.active_turn_id is None and current.revision == 3
    assert reopened.read_turn(req.session, req.operation_id) == result
    assert len(reopened.intents(req.session)) == 1
    assert reopened.intents(req.session)[0]["kind"] == "mailbox"
    replay = RuntimeService(reopened, backend).execute(current, replace(req, deadline=200))
    assert replay == result and backend.calls == 1


def test_crash_before_terminal_commit_rolls_back_result_intent_and_snapshot(tmp_path):
    repo, snapshot, req = setup(tmp_path)
    repo.begin(snapshot, req)
    admitted = repo.read_current("session")
    with pytest.raises(WorkingRepositoryError, match="injected"):
        repo.finish(req, {"accepted": True, "output_text": "answer"}, fail_before_commit=True)
    reopened = RuntimeRepository(repo.root)
    assert reopened.read_turn(req.session, req.operation_id).status == "submitted"
    assert reopened.read_current("session") == admitted
    assert reopened.intents(req.session) == ()
    backend = Backend()
    pending = RuntimeService(reopened, backend).execute(admitted, req)
    assert pending.status == "submitted" and backend.calls == 0
    reconciled = reopened.reconcile(req.session)
    assert reconciled[0].result["disposition"] == "unknown"
    assert reconciled[0].result["unresolved_effects"] is True
    assert reopened.intents(req.session)[0]["kind"] == "parked"
    assert reopened.reconcile(req.session) == ()
    assert len(reopened.intents(req.session)) == 1


def test_foreign_result_and_throwing_backend_never_accept_or_leak(tmp_path):
    repo, snapshot, req = setup(tmp_path)

    class Foreign:
        def invoke(self, request):
            return Backend().invoke(replace(request, operation_id="foreign"))

    result = RuntimeService(repo, Foreign()).execute(snapshot, req)
    assert result.result["accepted"] is False
    assert result.result["disposition"] == "unknown"
    current = repo.read_current("session")

    class Poison(Exception):
        def __str__(self):
            raise AssertionError("FAKE-SECRET-SENTINEL")

    class Fails:
        def invoke(self, request):
            raise Poison()

    result = RuntimeService(repo, Fails()).execute(current, replace(req, operation_id="next"))
    assert "SENTINEL" not in str(result.to_dict())
    assert result.result["unresolved_effects"]


def test_changed_request_and_wrong_owner_do_not_reuse_operation(tmp_path):
    repo, snapshot, req = setup(tmp_path)
    RuntimeService(repo, Backend()).execute(snapshot, req)
    with pytest.raises(ContractError, match="different request"):
        repo.begin(snapshot, replace(req, input_text="changed"))
    with pytest.raises(ContractError, match="owner"):
        repo.read_turn(replace(req.session, agent_id="foreign"), req.operation_id)
    assert repo.intents(replace(req.session, agent_id="foreign")) == ()


def test_concurrent_turn_rejected_and_operator_cancel_fences_late_result(tmp_path):
    repo, snapshot, req = setup(tmp_path)
    repo.begin(snapshot, req)
    admitted = repo.read_current("session")
    with pytest.raises(WorkingSetConflict):
        repo.begin(admitted, replace(req, operation_id="competing"))
    WorkingContinuityService(repo).cancel(admitted)
    result = repo.finish(req, {"accepted": True, "output_text": "late answer"})
    assert result.result["accepted"] is False and result.result["reason"] == "superseded"
    assert repo.read_current("session").status == "cancelled"
    assert repo.intents(req.session)[0]["kind"] == "parked"


def test_live_execution_blocks_recovery_and_competing_driver(tmp_path):
    from ahc.infrastructure.working_repository import WorkingWriterBusy

    repo, snapshot, req = setup(tmp_path)

    class Interleaver(Backend):
        def invoke(self, request):
            other = RuntimeRepository(repo.root)
            with pytest.raises(WorkingWriterBusy):
                other.reconcile(request.session)
            with pytest.raises(WorkingWriterBusy):
                RuntimeService(other, Backend()).execute(snapshot, request)
            assert other.read_turn(request.session, request.operation_id).status == "submitted"
            assert other.intents(request.session) == ()
            return super().invoke(request)

    result = RuntimeService(repo, Interleaver()).execute(snapshot, req)
    assert result.result["accepted"] is True
    assert repo.reconcile(req.session) == ()


def test_long_backend_completion_identifiers_are_not_persisted(tmp_path):
    repo, snapshot, req = setup(tmp_path)

    class LongIdentifier:
        def invoke(self, request):
            return AgentTurnObservation(
                request, "completed", "fixture", 1, 1, "x" * 129, "turn", "answer"
            )

    result = RuntimeService(repo, LongIdentifier()).execute(snapshot, req)
    assert result.result["accepted"] is False and result.result["disposition"] == "unknown"


def test_new_answer_seal_requires_current_admission_but_existing_seal_is_idempotent(tmp_path):
    repo, snapshot, req = setup(tmp_path)
    accepted = RuntimeService(repo, Backend()).execute(snapshot, req)
    seal = repo.read_answer_seal(req.session, req.operation_id)
    repo.seal_observation(req, seal)  # An existing exact seal remains readable/idempotent.

    current = repo.read_current(req.session.ref.virtual_session_id)
    next_request = replace(req, operation_id="next")
    repo.begin(current, next_request)
    WorkingContinuityService(repo).cancel(repo.read_current(req.session.ref.virtual_session_id))
    with pytest.raises(ContractError, match="current active admission"):
        repo.seal_observation(next_request, seal)
    assert accepted.result["accepted"]
