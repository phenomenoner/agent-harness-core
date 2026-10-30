"""Runtime backup/recovery preserves revision custody and never grants a resend."""

import hashlib
import sqlite3
from contextlib import closing

import pytest
from test_memory_revision_service import correct, prepared

from ahc.application.memory_revision_service import MemoryRevisionService
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore


@pytest.mark.parametrize("outcome", ["succeeded", "unknown"])
def test_revision_recovery_preserves_result_and_blocks_new_generation_replay(tmp_path, outcome):
    service, session, _, original, calls, options = prepared(tmp_path)
    if outcome == "unknown":
        real = service.revise

        def lost(**request):
            real(**request)
            raise TimeoutError("reply lost")

        service.revise = lost
    result = correct(service, session, original)
    assert result["status"] == outcome
    repository = options["repository"]
    snapshot = repository.read_current(session.ref.virtual_session_id)
    with closing(sqlite3.connect(repository.db_path)) as conn:
        custody = conn.execute("SELECT * FROM memory_revision_operations").fetchone()
    backup, candidate, restored = (tmp_path / name for name in ("backup", "candidate", "restored"))
    receipt = backup_runtime_database(repository, backup)
    prepare_runtime_restore(backup, candidate)
    source_digest = hashlib.sha256(repository.db_path.read_bytes()).hexdigest()
    transformed = transform_runtime_restore(candidate, receipt["sha256"], (snapshot,),
                                              source_digest, restored)
    for database in (backup / "runtime.sqlite", restored / "recovered-candidate.sqlite"):
        with closing(sqlite3.connect(database)) as conn:
            assert conn.execute("SELECT * FROM memory_revision_operations").fetchone() == custody
    activate_runtime_restore(repository, tmp_path / "supervisor.lock", restored,
                             transformed["sha256"], receipt["sha256"], source_digest)
    successor = repository.read_current(session.ref.virtual_session_id).session
    assert successor != session
    recovered = MemoryRevisionService(**options)
    for identity in (session, successor):
        with pytest.raises(ContractError):
            correct(recovered, identity, original)
    # Exact old custody may be inspected by its owner, never translated into a
    # fresh dispatch grant for the successor generation.
    assert recovered.reconcile_only(session, result["revision_digest"])["status"] == "succeeded"
    with pytest.raises(ContractError):
        recovered.reconcile_only(successor, result["revision_digest"])
    assert len(calls) == 1
