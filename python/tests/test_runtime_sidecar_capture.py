"""Recover real crash-retained WAL/hot journals only on copied SQLite bundles."""

import sqlite3
import subprocess
import sys
from contextlib import closing

import pytest
from test_runtime_core import setup

from ahc.infrastructure.runtime_backup import _digest
from ahc.infrastructure.runtime_sidecar_capture import SUFFIXES, capture_runtime_sidecars
from ahc.infrastructure.working_db import WORKING_DB_FILENAME

CHILD = """
import os, sqlite3, sys
c = sqlite3.connect(sys.argv[1])
c.execute('CREATE TABLE recovery_probe (id INTEGER PRIMARY KEY, value TEXT)')
c.executemany('INSERT INTO recovery_probe VALUES(?,?)', [(i, 'a'*4096) for i in range(128)])
c.commit()
c.execute('PRAGMA cache_size=8')
c.execute('PRAGMA journal_mode=' + sys.argv[2])
c.execute('BEGIN IMMEDIATE')
c.execute("UPDATE recovery_probe SET value=?", ('b'*4096,))
if sys.argv[2] == 'WAL':
    c.commit()
os._exit(23)
"""


@pytest.mark.parametrize("mode", ["WAL", "DELETE"])
def test_original_bundle_unchanged_and_normalized_content_correct(tmp_path, mode):
    repo, _, _ = setup(tmp_path)
    database = repo.root / WORKING_DB_FILENAME
    child = subprocess.run([sys.executable, "-c", CHILD, str(database), mode],
                           capture_output=True, timeout=15)
    assert child.returncode == 23, child.stderr
    expected_suffix = "-wal" if mode == "WAL" else "-journal"
    assert (repo.root / (WORKING_DB_FILENAME + expected_suffix)).stat().st_size > 0
    before = {p.name: p.read_bytes() for suffix in SUFFIXES
              if (p := repo.root / (WORKING_DB_FILENAME + suffix)).exists()}
    destination = tmp_path / "capture"
    result = capture_runtime_sidecars(repo, tmp_path / "supervisor.lock", destination)
    after = {p.name: p.read_bytes() for suffix in SUFFIXES
             if (p := repo.root / (WORKING_DB_FILENAME + suffix)).exists()}
    assert before == after
    assert all((destination / "raw" / name).read_bytes() == content
               for name, content in before.items())
    normalized = destination / result["database"]
    assert _digest(normalized) == result["sha256"] and result["activation_allowed"] is False
    with closing(sqlite3.connect(normalized)) as connection:
        rows = connection.execute("SELECT value FROM recovery_probe").fetchall()
        assert rows == [(('b' if mode == 'WAL' else 'a') * 4096,)] * 128
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
