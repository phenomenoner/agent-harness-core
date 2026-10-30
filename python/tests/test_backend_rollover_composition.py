"""CLI generation rollover and durable adapter rebinding; no provider invocation."""

import json
from dataclasses import replace

import pytest
from test_runtime_cli import call
from test_runtime_core import Backend, setup

from ahc.application.runtime_service import RuntimeService
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict


def rollover(capsys, repo, owner):
    return call(
        capsys, "runtime-continue", "--runtime-root", str(repo.root),
        "--session-json", json.dumps(owner.to_dict()),
        "--successor-session-json", json.dumps(owner.successor().to_dict()),
        "--reason", "backend-rollover",
    )


def test_completed_turn_rolls_to_new_binding_without_losing_virtual_session(tmp_path, capsys):
    repo, initial, request = setup(tmp_path)
    before = WorkingContinuityService(repo).advance(
        initial, progress=("first adapter ready",), evidence_refs=("evidence-1",),
    )
    old_backend, new_backend = Backend(), Backend()
    previous = RuntimeService(repo, old_backend).execute(before, request)
    code, _ = rollover(capsys, repo, request.session)
    assert code == 0

    fresh = RuntimeRepository(repo.root)
    current = fresh.read_current("session")
    assert current.session == request.session.successor()
    assert current.goal == before.goal
    assert current.progress == before.progress and current.evidence_refs == before.evidence_refs
    assert current.continuation_reason == "backend-rollover"
    rebound = replace(request, session=current.session, operation_id="replacement-turn",
                      artifact_digest="c" * 64, configuration_digest="d" * 64)
    result = RuntimeService(fresh, new_backend).execute(current, rebound)
    assert result.result["accepted"] is True
    assert old_backend.calls == new_backend.calls == 1
    reopened = RuntimeRepository(repo.root)
    assert reopened.read_turn(request.session, request.operation_id) == previous
    assert reopened.read_turn(rebound.session, rebound.operation_id) == result
    assert result.request_digest != previous.request_digest
    assert RuntimeService(reopened, new_backend).execute(current, rebound) == result
    assert new_backend.calls == 1
    # A stale generation may read its history, but cannot start another effect.
    with pytest.raises(WorkingSetConflict):
        RuntimeService(reopened, old_backend).execute(
            before, replace(request, operation_id="stale-turn"),
        )
    with pytest.raises(ContractError):
        RuntimeService(reopened, new_backend).execute(
            current, replace(rebound, operation_id=request.operation_id),
        )
    assert old_backend.calls == new_backend.calls == 1


def test_pending_turn_blocks_rollover_and_reconciliation_preserves_unknown(tmp_path, capsys):
    repo, initial, request = setup(tmp_path)
    with repo.execution_lock():
        repo.begin(initial, request)
    admitted = repo.read_current("session")
    code, _ = rollover(capsys, repo, request.session)
    assert code == 2
    assert RuntimeRepository(repo.root).read_current("session") == admitted
    repo.reconcile(request.session)
    unknown = repo.read_turn(request.session, request.operation_id)
    assert unknown.result["disposition"] == "unknown"
    code, _ = rollover(capsys, repo, request.session)
    assert code == 0
    fresh = RuntimeRepository(repo.root)
    assert fresh.read_turn(request.session, request.operation_id) == unknown
    backend = Backend()
    assert RuntimeService(fresh, backend).execute(initial, request) == unknown
    assert backend.calls == 0
