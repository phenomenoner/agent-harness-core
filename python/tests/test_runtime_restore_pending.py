"""Explicit pending outcome reconciliation preserves bytes and rejects stale choices."""

import json

import pytest
from test_runtime_core import setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import (
    RestoreIndeterminate,
    activate_runtime_restore,
)
from ahc.infrastructure.runtime_restore_pending import (
    inspect_pending_restore,
    reconcile_pending_restore,
)
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


@pytest.mark.parametrize("outcome", ["source", "candidate", "rollback-damaged", "unknown"])
def test_exact_outcome_reconciliation_without_replacement(tmp_path, monkeypatch, outcome):
    repo, saved, _ = setup(tmp_path)
    database = repo.root / WORKING_DB_FILENAME
    original_digest = _digest(database)
    backup, prepared, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    receipt = transform_runtime_restore(prepared, manifest["sha256"], (saved,),
                                        original_digest, transformed)
    if outcome == "source":
        def fail(*_):
            raise OSError("fixture replacement failure")
        monkeypatch.setattr("ahc.infrastructure.runtime_restore_activation.os.replace", fail)
    with pytest.raises(RestoreIndeterminate):
        activate_runtime_restore(repo, tmp_path / "supervisor.lock", transformed,
            receipt["sha256"], manifest["sha256"], original_digest, fail_after_replace=True)
    pending = repo.root / "restore-pending.json"
    attempt = json.loads(pending.read_text())["attempt"]
    before, marker = database.read_bytes(), pending.read_bytes()
    if outcome == "rollback-damaged":
        (repo.root / "restore-attempts" / attempt / "rollback.sqlite").write_bytes(b"damaged")
        with pytest.raises(ContractError, match="rollback"):
            inspect_pending_restore(repo, attempt)
        assert pending.read_bytes() == marker
        return
    if outcome == "unknown":
        database.write_bytes(before + b"unknown trailer")
        report = inspect_pending_restore(repo, attempt)
        assert report["outcome"] == "unrecognized"
        with pytest.raises(ContractError):
            reconcile_pending_restore(repo, tmp_path / "supervisor.lock", attempt,
                                      report["observed_digest"], "accept-candidate")
        assert pending.read_bytes() == marker
        return
    report = inspect_pending_restore(repo, attempt)
    assert report["outcome"] == outcome
    assert pending.read_bytes() == marker and database.read_bytes() == before
    decision = "accept-candidate" if outcome == "candidate" else "retain-source"
    wrong = "retain-source" if outcome == "candidate" else "accept-candidate"
    with pytest.raises(ContractError):
        reconcile_pending_restore(repo, tmp_path / "supervisor.lock", attempt,
                                  report["observed_digest"], wrong)
    with pytest.raises(ContractError):
        reconcile_pending_restore(repo, tmp_path / "supervisor.lock", "f" * 32,
                                  report["observed_digest"], decision)
    assert pending.read_bytes() == marker
    result = reconcile_pending_restore(repo, tmp_path / "supervisor.lock", attempt,
                                       report["observed_digest"], decision)
    assert result["state"] == "reconciled" and not pending.exists()
    assert database.read_bytes() == before
    retained = repo.root / "restore-attempts" / attempt
    assert _digest(retained / "rollback.sqlite") == original_digest
    assert (retained / "resolution.json").is_file()
    with repo.execution_lock():
        assert repo.read_current("session").session.ref.generation == (
            2 if outcome == "candidate" else 1)
