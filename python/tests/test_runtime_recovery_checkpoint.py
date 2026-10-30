"""Disposable runtime composition of recovery checkpoint projection and startup."""

import shutil
import sqlite3
from contextlib import closing

import pytest
from test_runtime_core import setup
from test_runtime_workspace import Port

from ahc.adapters.wsl_python_checkpoint import validate_checkpoint
from ahc.application.runtime_service import RuntimeService
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME
from ahc.infrastructure.working_repository import WorkingSetCorrupt


@pytest.mark.parametrize("damage", [None, "mapping", "values"])
def test_recovered_checkpoint_opens_new_workspace_without_rewriting_original(tmp_path, damage):
    repo, saved, _ = setup(tmp_path)
    with RuntimeService(repo, None).open_workspace(saved, "work", Port) as workspace:
        workspace.port.value = 73
        original = workspace.checkpoint(workspace.identity, ("x",))
    backup, prepared, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    source = saved.continue_as(saved.session.successor(), "compaction")
    receipt = transform_runtime_restore(prepared, manifest["sha256"], (source,),
                                        "b" * 64, transformed)
    # Explicit disposable fixture installation; never a production activation command.
    root = tmp_path / "disposable-runtime"
    root.mkdir()
    shutil.copyfile(transformed / receipt["database"], root / WORKING_DB_FILENAME)
    candidate = RuntimeRepository(root)
    recovered = candidate.read_current("session")
    calls = []

    def factory(identity, checkpoint):
        validate_checkpoint(identity, checkpoint, initial=True)
        calls.append((identity, checkpoint))
        return Port(identity, checkpoint)

    if damage:
        with closing(sqlite3.connect(root / WORKING_DB_FILENAME)) as connection:
            if damage == "mapping":
                connection.execute("DELETE FROM runtime_recovery_transitions")
            else:
                connection.execute("UPDATE workspace_checkpoints SET values_json='{}'")
            connection.commit()
        with pytest.raises((ContractError, WorkingSetCorrupt)):
            RuntimeService(candidate, None).open_workspace(
                recovered, "work", factory, checkpoint_digest=original.digest)
        assert not calls
        return
    with RuntimeService(candidate, None).open_workspace(
            recovered, "work", factory, checkpoint_digest=original.digest) as new:
        assert new.port.value == 73
        assert new.identity.session == recovered.session
        assert new.identity.generation > original.origin.generation
        projected = calls[0][1]
        assert projected.digest != original.digest
        assert projected.origin.session == recovered.session
        assert projected.values_json == original.values_json
        fresh = new.checkpoint(new.identity, ("x",))
        assert fresh.values_json == original.values_json
        assert fresh.origin == new.identity
    reopened = RuntimeRepository(root)
    assert reopened.read_workspace_checkpoint(
        recovered.session, "work", original.digest) == projected
    with closing(sqlite3.connect(root / WORKING_DB_FILENAME)) as connection:
        stored = connection.execute("SELECT origin_json,values_json FROM workspace_checkpoints "
                                    "WHERE digest=?", (original.digest,)).fetchone()
        assert stored[1] == original.values_json
        assert stored[0] == canonical_json(original.origin.to_dict()).decode()
    assert repo.read_workspace_checkpoint(saved.session, "work", original.digest) == original
    with pytest.raises(ContractError):
        reopened.read_workspace_checkpoint(recovered.session, "different", original.digest)
