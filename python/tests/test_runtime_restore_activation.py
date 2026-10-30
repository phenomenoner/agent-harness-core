"""Disposable offline activation preserves rollback and fences interrupted execution."""

import pytest
from test_runtime_core import setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import (
    RestoreIndeterminate,
    activate_runtime_restore,
)
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


@pytest.mark.parametrize("mode", ["success", "interrupted", "false-source", "replace-failed"])
def test_offline_activation_or_pending_no_replay(tmp_path, mode, monkeypatch):
    repo, saved, _ = setup(tmp_path)
    active = repo.root / WORKING_DB_FILENAME
    source_digest = _digest(active)
    original = active.read_bytes()
    backup, prepared, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    source = (saved.continue_as(saved.session.successor(), "compaction")
              if mode == "false-source" else saved)
    receipt = transform_runtime_restore(prepared, manifest["sha256"], (source,),
                                        source_digest, transformed)
    args = (repo, tmp_path / "supervisor.lock", transformed, receipt["sha256"],
            manifest["sha256"], source_digest)
    if mode == "false-source":
        with pytest.raises(ContractError, match="source snapshot"):
            activate_runtime_restore(*args)
        assert active.read_bytes() == original
        assert not (repo.root / "restore-attempts").exists()
        return
    if mode == "replace-failed":
        def fail(*_):
            raise PermissionError("fixture held handle")
        monkeypatch.setattr("ahc.infrastructure.runtime_restore_activation.os.replace", fail)
    if mode in {"interrupted", "replace-failed"}:
        with pytest.raises(RestoreIndeterminate):
            activate_runtime_restore(*args, fail_after_replace=True)
        assert (repo.root / "restore-pending.json").is_file()
        with pytest.raises(ContractError, match="reconciliation"):
            with repo.execution_lock():
                pytest.fail("pending replacement allowed execution")
        with pytest.raises(ContractError, match="reconciliation"):
            activate_runtime_restore(*args)
    else:
        result = activate_runtime_restore(*args)
        assert result["state"] == "activated"
        assert not (repo.root / "restore-pending.json").exists()
        with repo.execution_lock():
            assert repo.read_current("session").session.ref.generation == 2
    attempts = list((repo.root / "restore-attempts").iterdir())
    assert len(attempts) == 1
    assert (attempts[0] / "rollback.sqlite").read_bytes() == original
    assert (attempts[0] / "completed.json").exists() == (mode == "success")
    assert _digest(active) == (source_digest if mode == "replace-failed" else receipt["sha256"])
