"""Disposable host primitive probe, not a runtime activation implementation."""

import os
import queue
import shutil
import subprocess
import sys
import threading

import pytest
from test_runtime_core import setup

from ahc.application.working_continuity import WorkingContinuityService
from ahc.infrastructure.runtime_backup import _digest
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_db import WORKING_DB_FILENAME
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_offline_replace_and_retained_rollback_under_existing_locks(tmp_path):
    source_root, candidate_root = tmp_path / "source", tmp_path / "candidate"
    source_root.mkdir()
    candidate_root.mkdir()
    source, saved, _ = setup(source_root)
    candidate, before, _ = setup(candidate_root)
    successor = WorkingContinuityService(candidate).continue_generation(
        before, before.session.successor(), "compaction")
    active = source.root / WORKING_DB_FILENAME
    staged = candidate.root / WORKING_DB_FILENAME
    rollback, replacement = tmp_path / "rollback.sqlite", tmp_path / "replacement.sqlite"
    original_digest, replacement_digest = _digest(active), _digest(staged)
    with source.execution_lock(), source.writer_lock():
        # Probe's precondition: all fixture SQLite connections are closed, no WAL sidecars.
        assert all(not (source.root / (WORKING_DB_FILENAME + suffix)).exists()
                   for suffix in ("-wal", "-shm", "-journal"))
        with active.open("rb") as original, rollback.open("xb") as retained:
            shutil.copyfileobj(original, retained)
            retained.flush()
            os.fsync(retained.fileno())
        assert _digest(active) == original_digest == _digest(rollback)
        os.replace(staged, active)
        assert _digest(active) == replacement_digest
        assert RuntimeRepository(source.root).read_current("session") == successor
        # Retain rollback generation; restore from a distinct copy while ownership is held.
        shutil.copyfile(rollback, replacement)
        os.replace(replacement, active)
        assert _digest(active) == _digest(rollback) == original_digest
        assert RuntimeRepository(source.root).read_current("session") == saved
    assert not staged.exists() and rollback.exists()


@pytest.mark.parametrize("lock", ["execution_lock", "writer_lock"])
def test_other_process_owner_rejects_before_any_copy_or_replacement(tmp_path, lock):
    repo, _, _ = setup(tmp_path)
    active = repo.root / WORKING_DB_FILENAME
    before = active.read_bytes()
    marker = tmp_path / "would-replace"
    script = """
import sys
from ahc.infrastructure.runtime_repository import RuntimeRepository
repo = RuntimeRepository(sys.argv[1])
with getattr(repo, sys.argv[2])():
    print('held', flush=True)
    sys.stdin.readline()
"""
    child = subprocess.Popen([sys.executable, "-c", script, str(repo.root), lock],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
    ready = queue.SimpleQueue()
    threading.Thread(target=lambda: ready.put(child.stdout.readline()), daemon=True).start()
    try:
        assert ready.get(timeout=5).strip() == "held"
        assert child.poll() is None
        with pytest.raises(WorkingWriterBusy):
            with repo.execution_lock(), repo.writer_lock():
                marker.write_text("effect", encoding="utf-8")
        assert not marker.exists() and active.read_bytes() == before
    finally:
        try:
            _, stderr = child.communicate("release\n", timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate(timeout=5)
            raise
    assert child.returncode == 0, stderr
    with repo.execution_lock(), repo.writer_lock():
        assert active.read_bytes() == before
