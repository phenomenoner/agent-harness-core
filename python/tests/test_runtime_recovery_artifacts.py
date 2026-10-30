"""Read original artifact bytes through an exact, persisted recovery transition."""

import sqlite3
from contextlib import closing, contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_artifacts import state

from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_repository import WorkingSetCorrupt


@pytest.mark.parametrize("damage", [None, "digest", "saved", "recovered", "missing"])
def test_exact_recovery_access_and_provenance_damage(tmp_path, damage):
    repo, saved, producer, artifacts = state(tmp_path)
    original = artifacts.publish(producer, b"saved bytes", "text/plain")
    source = saved.continue_as(saved.session.successor(), "compaction")
    backup, prepared, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    receipt = transform_runtime_restore(prepared, manifest["sha256"], (source,),
                                        "b" * 64, transformed)
    database = transformed / receipt["database"]

    @contextmanager
    def read():
        with closing(sqlite3.connect(database)) as connection:
            connection.row_factory = sqlite3.Row
            yield connection

    candidate = ArtifactRepository(SimpleNamespace(_read=read, _load_current=repo._load_current))
    with read() as connection:
        recovered = repo._load_current(connection, "session")
    if damage:
        with read() as connection:
            if damage == "digest":
                connection.execute("UPDATE runtime_recovery_transitions SET transition_digest=?",
                                   ("c" * 64,))
            elif damage in {"saved", "recovered"}:
                digest = saved.snapshot_digest if damage == "saved" else recovered.snapshot_digest
                connection.execute("UPDATE snapshots SET snapshot_json='{}' "
                                   "WHERE snapshot_digest=?", (digest,))
            else:
                connection.execute("DELETE FROM runtime_recovery_transitions")
            connection.commit()
        with pytest.raises((WorkingSetCorrupt, ContractError)):
            candidate.read(recovered.session, original.artifact_id)
        return
    assert candidate.read(recovered.session, original.artifact_id) == (original, b"saved bytes")
    assert artifacts.read(saved.session, original.artifact_id) == (original, b"saved bytes")
    for wrong in (source.session, recovered.session.successor(),
                  replace(recovered.session, agent_id="other")):
        with pytest.raises(ContractError):
            candidate.read(wrong, original.artifact_id)
    # Ordinary continuation still does not confer inherited artifact access.
    WorkingContinuityService(repo).continue_generation(saved, source.session, "compaction")
    with pytest.raises(ContractError):
        artifacts.read(source.session, original.artifact_id)
