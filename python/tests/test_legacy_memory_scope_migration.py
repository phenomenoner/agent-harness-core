import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "scope_migration", Path(__file__).parents[1] / "tools/migrate_legacy_memory_scope.py"
)
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


def fixture(path):
    details = [
        '{"agent_id":"main","text":"original"}',
        '{"agent_id":"main","scope":null}',
        '{"agent_id":"other"}',
        "{}",
        '{"agent_id":"main","scope":"existing"}',
        '{"agent_id":"other","agent_id":"main"}',
    ]
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE observations(id INTEGER PRIMARY KEY,summary TEXT,detail_json TEXT)"
        )
        conn.executemany(
            "INSERT INTO observations VALUES (?,?,?)",
            [(i, "body", d) for i, d in enumerate(details, 1)],
        )
    return details


def read(path):
    with sqlite3.connect(path) as conn:
        return conn.execute("SELECT * FROM observations ORDER BY id").fetchall()


def test_mapping_and_exact_json_restoration(tmp_path):
    source, target, restored = [
        tmp_path / name for name in ("source.db", "target.db", "restored.db")
    ]
    fixture(source)
    original = read(source)
    digest = migration.sha256(source)
    result = migration.migrate(source, target, expected=digest, owner="main", scope="agent:main")
    assert result["counts"] == dict(mapped=2, retained_scoped=1, retained_foreign=1, unresolved=2)
    assert migration.sha256(source) == digest
    assert read(target)[2:] == original[2:]
    assert json.loads(read(target)[0][2]) == {
        "agent_id": "main",
        "text": "original",
        "scope": "agent:main",
    }
    migration.restore_copy(target, restored, expected=result["target_sha256"])
    assert read(restored) == original


@pytest.mark.parametrize("failure", ["hash", "sidecar", "target-sidecar", "occupied"])
def test_preflight_refuses(tmp_path, failure):
    source, target = tmp_path / "source.db", tmp_path / "target.db"
    fixture(source)
    digest = migration.sha256(source)
    if failure == "hash":
        digest = "0" * 64
    if failure == "sidecar":
        Path(str(source) + "-wal").touch()
    if failure == "target-sidecar":
        Path(str(target) + "-wal").touch()
    if failure == "occupied":
        target.write_bytes(b"keep")
    with pytest.raises((ValueError, FileExistsError)):
        migration.migrate(source, target, expected=digest, owner="main", scope="agent:main")
    assert target.read_bytes() == b"keep" if failure == "occupied" else not target.exists()


def test_restore_refuses_newer_row(tmp_path):
    source, target, restored = [
        tmp_path / name for name in ("source.db", "target.db", "restored.db")
    ]
    fixture(source)
    migration.migrate(
        source, target, expected=migration.sha256(source), owner="main", scope="agent:main"
    )
    with sqlite3.connect(target) as conn:
        conn.execute("UPDATE observations SET detail_json='{}' WHERE id=1")
    with pytest.raises(ValueError, match="mapped row changed"):
        migration.restore_copy(target, restored, expected=migration.sha256(target))
    assert read(restored) == read(target)
