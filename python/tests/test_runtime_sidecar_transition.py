"""Original bundle normalization and process-loss continuation retain every source byte."""

import json
import sqlite3
import subprocess
import sys
from contextlib import closing

import pytest
from test_runtime_core import setup
from test_runtime_sidecar_capture import CHILD

from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_sidecar_capture import capture_runtime_sidecars
from ahc.infrastructure.runtime_sidecar_transition import (
    normalize_runtime_source,
    resume_sidecar_transition,
)
from ahc.infrastructure.working_db import WORKING_DB_FILENAME
from ahc.infrastructure.working_repository import WorkingRepositoryError

TRANSITION = """
import os, sys
from pathlib import Path
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_sidecar_transition import normalize_runtime_source
root, capture, capture_digest, normalized_digest, phase = sys.argv[1:]
rename, replace = os.rename, os.replace
def move(source, target):
    rename(source, target)
    if phase == 'retired-db' and Path(source).name == 'working.db':
        os._exit(23)
def install(source, target):
    replace(source, target)
    if phase == 'installed':
        os._exit(23)
os.rename, os.replace = move, install
normalize_runtime_source(RuntimeRepository(root), Path(root)/'supervisor.lock',
                         capture, capture_digest, normalized_digest)
raise AssertionError('crash seam was not reached')
"""


@pytest.mark.parametrize("mode", ["WAL", "DELETE"])
@pytest.mark.parametrize("phase", [None, "retired-db", "installed"])
def test_normalize_original_bundle_and_resume_exact_attempt(tmp_path, mode, phase):
    repo, saved, _ = setup(tmp_path)
    database = repo.root / WORKING_DB_FILENAME
    crashed = subprocess.run([sys.executable, "-c", CHILD, str(database), mode],
                             capture_output=True, timeout=15)
    assert crashed.returncode == 23
    capture = tmp_path / "capture"
    receipt = capture_runtime_sidecars(repo, repo.root / "supervisor.lock", capture)
    if phase:
        crashed = subprocess.run([sys.executable, "-c", TRANSITION, str(repo.root), str(capture),
            receipt["capture_digest"], receipt["sha256"], phase], capture_output=True, timeout=15)
        assert crashed.returncode == 23, crashed.stderr
        pending = repo.root / "sidecar-pending.json"
        attempt = json.loads(pending.read_text())["attempt"]
        reopened = RuntimeRepository(repo.root, allow_pending_sidecar=True)
        with pytest.raises(WorkingRepositoryError):
            reopened.read_current("session")
        result = resume_sidecar_transition(reopened, repo.root / "supervisor.lock", attempt)
    else:
        result = normalize_runtime_source(repo, repo.root / "supervisor.lock", capture,
                                          receipt["capture_digest"], receipt["sha256"])
    assert result["state"] == "normalized" and result["restored_generation"] is False
    assert not (repo.root / "sidecar-pending.json").exists()
    assert RuntimeRepository(repo.root).read_current("session") == saved
    retained = repo.root / "sidecar-attempts" / result["attempt"] / "original"
    assert {path.name for path in retained.iterdir()} == set(receipt["files"])
    for name in receipt["files"]:
        assert (retained / name).read_bytes() == (capture / "raw" / name).read_bytes()
    with closing(sqlite3.connect(database)) as connection:
        rows = connection.execute("SELECT value FROM recovery_probe").fetchall()
        assert rows == [(('b' if mode == 'WAL' else 'a') * 4096,)] * 128
