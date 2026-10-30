import sqlite3

import pytest
from test_workspace_tools import setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_backup import backup_runtime_database, inspect_runtime_backup
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_backup_preserves_checkpoint_artifact_and_source(tmp_path):
    store, workspace, tools = setup(tmp_path)
    try:
        tools.invoke("workspace.execute", {
            "expected": workspace.identity.to_dict(), "code": "x=42"})
        checkpoint = tools.invoke("workspace.checkpoint", {
            "expected": workspace.identity.to_dict(), "names": ["x"]})
        assert checkpoint["disposition"] == "observed"
        manifest = ArtifactRepository(store).publish(workspace.identity, b"retained", "text/plain")
        before = store.db_path.read_bytes()
        destination = tmp_path / "backup"
        receipt = backup_runtime_database(store, destination)
        assert inspect_runtime_backup(destination) == receipt
        assert receipt["restorable_runtime"] is False
        assert store.db_path.read_bytes() == before
        conn = sqlite3.connect(destination / "runtime.sqlite")
        try:
            assert conn.execute("SELECT content FROM runtime_artifacts WHERE artifact_id=?",
                                (manifest.artifact_id,)).fetchone()[0] == b"retained"
            assert conn.execute("SELECT count(*) FROM snapshots").fetchone()[0] > 0
            assert conn.execute("SELECT count(*) FROM workspace_checkpoints").fetchone()[0] == 1
        finally:
            conn.close()
        with pytest.raises(FileExistsError):
            backup_runtime_database(store, destination)
        assert inspect_runtime_backup(destination) == receipt
        with (destination / "runtime.sqlite").open("ab") as output:
            output.write(b"corrupt")
        with pytest.raises(ContractError):
            inspect_runtime_backup(destination)
    finally:
        workspace.close()


def test_busy_runtime_and_nested_destination_reject_before_creation(tmp_path):
    store, workspace, _ = setup(tmp_path)
    try:
        with store.execution_lock(), pytest.raises(WorkingWriterBusy):
            backup_runtime_database(store, tmp_path / "busy-backup")
        assert not (tmp_path / "busy-backup").exists()
        with pytest.raises(ContractError):
            backup_runtime_database(store, store.root / "nested")
        assert not (store.root / "nested").exists()
    finally:
        workspace.close()
