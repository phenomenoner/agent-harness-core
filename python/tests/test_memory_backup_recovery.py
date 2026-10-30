"""Runtime recovery retains memory proposal evidence without renewing approval."""

import hashlib
import sqlite3
from contextlib import closing

import pytest
from test_memory_store_approvals import prepared

from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_store_approvals import MemoryStoreApprovals
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_repository import WorkingSetConflict


@pytest.mark.parametrize("state", ["approved", "in-flight", "unknown", "succeeded"])
def test_backup_and_recovery_preserve_proposal_and_fence_old_session(tmp_path, state):
    repo, snapshot, owner, digest = prepared(tmp_path)
    owner.approve(snapshot.session, digest, approval_expires=150)
    if state != "approved":
        owner.consume(snapshot.session, digest)
    if state in {"unknown", "succeeded"}:
        owner.finish(snapshot.session, digest, state)
    with closing(sqlite3.connect(repo.db_path)) as connection:
        original = connection.execute(
            "SELECT * FROM memory_store_proposals WHERE digest=?", (digest,)
        ).fetchone()
    backup, candidate, recovered = (tmp_path / name for name in ("backup", "candidate", "new"))
    receipt = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, candidate)
    source_digest = hashlib.sha256(repo.db_path.read_bytes()).hexdigest()
    transformed = transform_runtime_restore(
        candidate, receipt["sha256"], (snapshot,), source_digest, recovered)
    for database in (backup / "runtime.sqlite", recovered / "recovered-candidate.sqlite"):
        with closing(sqlite3.connect(database)) as connection:
            assert connection.execute(
                "SELECT * FROM memory_store_proposals WHERE digest=?", (digest,)
            ).fetchone() == original
    # Test the same generation admission used by approval consumption against
    # the transformed candidate; do not activate it as a runtime.
    with closing(sqlite3.connect(recovered / "recovered-candidate.sqlite")) as connection:
        with pytest.raises(WorkingSetConflict):
            owner._current(connection, snapshot.session)
    assert owner.query(snapshot.session, digest)["state"] == state
    activate_runtime_restore(repo, tmp_path / "supervisor.lock", recovered,
                             transformed["sha256"], receipt["sha256"], source_digest)
    successor = repo.read_current(snapshot.session.ref.virtual_session_id).session
    assert successor != snapshot.session
    fresh = MemoryStoreApprovals(repo, clock=lambda: 110)
    before = repo.db_path.read_bytes()
    for session in (snapshot.session, successor):
        with pytest.raises((ContractError, WorkingSetConflict)):
            fresh.approve(session, digest, approval_expires=140)
        with pytest.raises((ContractError, WorkingSetConflict)):
            fresh.consume(session, digest)
    assert fresh.query(snapshot.session, digest)["state"] == state
    assert repo.db_path.read_bytes() == before
