"""Repeated recovery retains original content only across complete verified edges."""

import shutil
import sqlite3
from contextlib import closing

import pytest
from test_artifacts import state
from test_runtime_workspace import Port

from ahc.adapters.wsl_python_checkpoint import validate_checkpoint
from ahc.application.runtime_service import RuntimeService
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceCheckpoint
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME
from ahc.infrastructure.working_repository import WorkingSetCorrupt


@pytest.mark.parametrize("damage", [None, "missing-ancestor", "corrupt-ancestor", "ordinary-gap"])
def test_twice_recovered_content_and_checkpoint_startup(tmp_path, damage):
    repo, saved, producer, artifacts = state(tmp_path)
    manifest = artifacts.publish(producer, b"original", "text/plain")
    checkpoint = WorkspaceCheckpoint(producer, '{"x":91}')
    repo.save_workspace_checkpoint(saved, checkpoint)
    original_repo = repo
    first_digest = None
    for index in range(2):
        if index == 1 and damage == "ordinary-gap":
            before = repo.read_current("session")
            WorkingContinuityService(repo).continue_generation(
                before, before.session.successor(), "compaction")
        phase = tmp_path / str(index)
        phase.mkdir()
        backup, prepared, transformed = (phase / name for name in ("backup", "prep", "new"))
        receipt = backup_runtime_database(repo, backup)
        prepare_runtime_restore(backup, prepared)
        source = repo.read_current("session")
        result = transform_runtime_restore(prepared, receipt["sha256"], (source,),
                                           "b" * 64, transformed)
        root = phase / "disposable-runtime"
        root.mkdir()
        shutil.copyfile(transformed / result["database"], root / WORKING_DB_FILENAME)
        repo = RuntimeRepository(root)
        if index == 0:
            with repo._read() as connection:
                first_digest = connection.execute(
                    "SELECT transition_digest FROM runtime_recovery_transitions").fetchone()[0]
    current = repo.read_current("session")
    assert current.session.ref.generation == (4 if damage == "ordinary-gap" else 3)
    if damage:
        with closing(sqlite3.connect(repo.root / WORKING_DB_FILENAME)) as connection:
            if damage == "missing-ancestor":
                connection.execute("DELETE FROM runtime_recovery_transitions "
                                   "WHERE transition_digest=?", (first_digest,))
            elif damage == "corrupt-ancestor":
                connection.execute("UPDATE runtime_recovery_transitions SET transition_json='{}' "
                                   "WHERE transition_digest=?", (first_digest,))
            connection.commit()
        with pytest.raises((ContractError, WorkingSetCorrupt)):
            ArtifactRepository(repo).read(current.session, manifest.artifact_id)
        with pytest.raises((ContractError, WorkingSetCorrupt)):
            repo.read_workspace_checkpoint(current.session, "workspace", checkpoint.digest)
        return
    assert ArtifactRepository(repo).read(current.session, manifest.artifact_id) == (
        manifest, b"original")

    def factory(identity, projected):
        validate_checkpoint(identity, projected, initial=True)
        return Port(identity, projected)

    with RuntimeService(repo, None).open_workspace(current, "workspace", factory,
            checkpoint_digest=checkpoint.digest) as workspace:
        assert workspace.port.value == 91
        assert workspace.identity.session == current.session
    assert ArtifactRepository(original_repo).read(saved.session, manifest.artifact_id) == (
        manifest, b"original")
