"""Backup custody preserves a sealed answer without activating an interrupted turn."""

import json
import sqlite3
from contextlib import closing

from test_runtime_core import Backend, setup

from ahc.application.runtime_service import RuntimeService
from ahc.domain.runtime_turn import accept_observation, answer_seal_projection
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore import inspect_runtime_restore, prepare_runtime_restore


def test_sealed_submitted_turn_survives_backup_and_prepared_restore_without_replay(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    repo.begin(snapshot, request)
    result = accept_observation(request, Backend().invoke(request))
    repo.seal_observation(request, result)
    expected = answer_seal_projection(result)

    backup = tmp_path / "backup"
    manifest = backup_runtime_database(repo, backup)
    assert "runtime_answer_seals" in manifest["tables"]
    candidate = tmp_path / "prepared"
    receipt = prepare_runtime_restore(backup, candidate)
    assert receipt["activation_allowed"] is False
    assert inspect_runtime_restore(candidate, manifest["sha256"]) == receipt
    for database in (backup / "runtime.sqlite", candidate / "restore-candidate.sqlite"):
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
            seal = connection.execute(
                "SELECT result_json FROM runtime_answer_seals WHERE operation_id=?",
                (request.operation_id,),
            ).fetchone()
            assert json.loads(seal[0]) == expected
            assert connection.execute(
                "SELECT status FROM runtime_turns WHERE operation_id=?", (request.operation_id,),
            ).fetchone() == ("submitted",)

    reopened = RuntimeRepository(repo.root)
    backend = Backend()
    pending = RuntimeService(reopened, backend).execute(reopened.read_current("session"), request)
    assert pending.status == "submitted"
    assert backend.calls == 0
    assert reopened.intents(request.session) == ()
    assert reopened.read_answer_seal(request.session, request.operation_id) == expected
    assert reopened.reconcile(request.session)[0].result["disposition"] == "unknown"
    assert reopened.read_answer_seal(request.session, request.operation_id) == expected
