"""Actual process loss at replacement boundaries, with no Python cleanup handlers."""

import json
import subprocess
import sys

import pytest
from test_runtime_core import setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_pending import (
    inspect_pending_restore,
    reconcile_pending_restore,
)
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME

CHILD = """
import os, sys
from pathlib import Path
from ahc.infrastructure import runtime_restore_activation as activation
from ahc.infrastructure.runtime_repository import RuntimeRepository
root, candidate, candidate_digest, backup_digest, source_digest, phase = sys.argv[1:]
original_replace, original_write = os.replace, activation._write_json
def replacement(*args):
    if phase == 'before-replace':
        os._exit(23)
    original_replace(*args)
    if phase == 'after-replace':
        os._exit(23)
def write(path, value):
    original_write(path, value)
    if phase == 'after-completed' and path.name == 'completed.json':
        os._exit(23)
    if phase == 'after-resolution' and path.name == 'resolution.json':
        os._exit(23)
os.replace, activation._write_json = replacement, write
store = RuntimeRepository(root)
try:
    activation.activate_runtime_restore(store, Path(root)/'supervisor.lock',
        candidate, candidate_digest, backup_digest, source_digest,
        fail_after_replace=phase == 'after-resolution')
except activation.RestoreIndeterminate:
    if phase != 'after-resolution':
        raise
    import json
    from ahc.infrastructure import runtime_restore_pending as pending
    pending._write_json = write
    attempt = json.loads((Path(root)/'restore-pending.json').read_text())['attempt']
    pending.reconcile_pending_restore(store, Path(root)/'supervisor.lock', attempt,
                                      candidate_digest, 'accept-candidate')
raise AssertionError('crash seam was not reached')
"""


@pytest.mark.parametrize("phase", ["before-replace", "after-replace", "after-completed",
                                  "after-resolution"])
def test_crash_reopen_and_explicit_resolution(tmp_path, phase):
    repo, saved, _ = setup(tmp_path)
    original_digest = _digest(repo.root / WORKING_DB_FILENAME)
    backup, prepared, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    receipt = transform_runtime_restore(prepared, manifest["sha256"], (saved,),
                                        original_digest, transformed)
    result = subprocess.run([sys.executable, "-c", CHILD, str(repo.root), str(transformed),
        receipt["sha256"], manifest["sha256"], original_digest, phase],
        capture_output=True, text=True, timeout=15)
    assert result.returncode == 23, result.stderr
    reopened = RuntimeRepository(repo.root)
    pending = repo.root / "restore-pending.json"
    attempt = json.loads(pending.read_text())["attempt"]
    report = inspect_pending_restore(reopened, attempt)
    expected = "source" if phase == "before-replace" else "candidate"
    assert report["outcome"] == expected
    with pytest.raises(ContractError, match="reconciliation"):
        with reopened.execution_lock():
            pytest.fail("crashed activation allowed new execution")
    retained = repo.root / "restore-attempts" / attempt
    assert _digest(retained / "rollback.sqlite") == original_digest
    assert (retained / "completed.json").exists() == (phase == "after-completed")
    assert (retained / "resolution.json").exists() == (phase == "after-resolution")
    before = (repo.root / WORKING_DB_FILENAME).read_bytes()
    reconcile_pending_restore(reopened, repo.root / "supervisor.lock", attempt,
        report["observed_digest"], "retain-source" if expected == "source" else "accept-candidate")
    assert not pending.exists()
    assert (repo.root / WORKING_DB_FILENAME).read_bytes() == before
    with reopened.execution_lock():
        assert reopened.read_current("session").session.ref.generation == (
            1 if expected == "source" else 2)
