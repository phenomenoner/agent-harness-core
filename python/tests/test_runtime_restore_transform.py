"""Candidate-only recovery transformation preserves source and commits mappings atomically."""

import sqlite3
from contextlib import closing

import pytest
from test_runtime_core import setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME, decode_snapshot


@pytest.mark.parametrize("rollback", [False, True])
def test_transform_preserves_inputs_and_atomic_recovery(tmp_path, rollback):
    repo, saved, _ = setup(tmp_path)
    backup, prepared, transformed = tmp_path / "backup", tmp_path / "prepared", tmp_path / "new"
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    source = saved
    for _ in range(3):
        source = source.continue_as(source.session.successor(), "compaction")
    before = (prepared / "restore-candidate.sqlite").read_bytes()
    args = (prepared, manifest["sha256"], (source,), "b" * 64, transformed)
    if rollback:
        with pytest.raises(ContractError, match="before commit"):
            transform_runtime_restore(*args, fail_before_commit=True)
        assert not (transformed / "transformation.json").exists()
    else:
        receipt = transform_runtime_restore(*args)
        assert receipt["activation_allowed"] is False
        assert receipt["transitions"][0]["source_session"] == source.session.to_dict()
    database = transformed / "recovered-candidate.sqlite"
    assert not (transformed / WORKING_DB_FILENAME).exists()
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert not connection.execute("PRAGMA foreign_key_check").fetchall()
        row = connection.execute("SELECT s.snapshot_json FROM current_working_sets c "
            "JOIN snapshots s ON s.snapshot_digest=c.snapshot_digest").fetchone()
        current = decode_snapshot(row[0])
        assert current.session.ref.generation == (1 if rollback else 5)
        assert connection.execute("SELECT count(*) FROM snapshots").fetchone()[0] == (
            1 if rollback else 2)
    assert (prepared / "restore-candidate.sqlite").read_bytes() == before
    assert repo.read_current("session") == saved
