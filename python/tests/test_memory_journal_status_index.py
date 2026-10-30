"""Real SQLite status/persistence checks; embeddings/projection are fixture-only."""

import json
import os
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import portalocker
import pytest
from test_memory_journal import Embedder, Projection, observation

from ahc.adapters.local_embedding import MODEL_IDENTITY
from ahc.adapters.mempalace_identity import mempalace_identity
from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_journal import MemoryJournal

INDEX_NAME = "records_pending_status"


@pytest.fixture
def journal(tmp_path):
    return MemoryJournal(
        tmp_path / "journal", embedder=Embedder(), projection=Projection(), create=True
    )


def table_state(journal):
    with journal._connect() as db:
        names = [
            row[0]
            for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
        ]
        return {
            name: sorted((tuple(row) for row in db.execute(f'SELECT * FROM "{name}"')), key=repr)
            for name in names
        }


def schema_state(journal):
    with journal._connect() as db:
        return [
            tuple(row)
            for row in db.execute("SELECT type,name,sql FROM sqlite_schema ORDER BY name")
        ]


def legacy_without_index(journal):
    # Authored legacy fixture: same v1 tables/content, no new derived index.
    with journal._connect() as db:
        db.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")


def test_actual_status_pending_query_uses_covering_index(journal, monkeypatch):
    for index in range(128):
        journal.store(
            scope="alice" if index % 2 else "bob",
            operation_id=f"wide-{index}",
            observation=observation("wide-fixture-" + "x" * 8192),
            index=False,
        )
    with journal._connect() as db:
        db.execute("UPDATE records SET indexed=1")

    statements = []
    original_connect = journal._connect

    @contextmanager
    def traced_connect():
        with original_connect() as db:
            db.set_trace_callback(statements.append)
            yield db

    monkeypatch.setattr(journal, "_connect", traced_connect)
    assert journal.status()["status"] == "ready"
    pending_sql = next(sql for sql in statements if "FROM records WHERE indexed=0" in sql)
    with original_connect() as db:
        plan = [row[3] for row in db.execute("EXPLAIN QUERY PLAN " + pending_sql)]
    assert any(f"COVERING INDEX {INDEX_NAME}" in row for row in plan), plan


def test_legacy_read_only_reopen_then_explicit_preparation_preserves_content(journal):
    original = journal.store(scope="alice", operation_id="original", observation=observation())
    legacy_without_index(journal)
    before_tables, before_schema = table_state(journal), schema_state(journal)
    before_bytes = journal.database.read_bytes()
    reopened = MemoryJournal(journal.root, embedder=Embedder(), projection=journal.projection)
    assert reopened.status()["status"] == "ready"
    assert reopened.get(scope="alice", record_ref=original["record_ref"]) == original
    assert reopened.database.read_bytes() == before_bytes
    assert schema_state(reopened) == before_schema

    prepared = MemoryJournal(
        journal.root, embedder=Embedder(), projection=journal.projection, create=True
    )
    assert prepared.store_id == journal.store_id
    assert table_state(prepared) == before_tables
    prepared_schema = schema_state(prepared)
    added = [row for row in prepared_schema if row not in before_schema]
    assert len(added) == 1 and added[0][:2] == ("index", INDEX_NAME), added
    assert "WHERE indexed=0" in added[0][2]
    again = MemoryJournal(
        journal.root, embedder=Embedder(), projection=journal.projection, create=True
    )
    assert table_state(again) == before_tables
    assert schema_state(again) == prepared_schema
    assert (
        again.reconcile(
            scope="alice", operation_id="original", request_digest=original["request_digest"]
        )
        == original
    )


def test_identity_refusal_does_not_prepare_existing_journal(journal):
    class DifferentEmbedder(Embedder):
        identity = "different-fixture-embedding"

    legacy_without_index(journal)
    before = schema_state(journal), table_state(journal), journal.database.read_bytes()
    with pytest.raises(ContractError, match="identity mismatch"):
        MemoryJournal(
            journal.root, embedder=DifferentEmbedder(), projection=Projection(), create=True
        )
    assert (schema_state(journal), table_state(journal), journal.database.read_bytes()) == before


def test_writer_exclusion_precedes_index_preparation(journal):
    legacy_without_index(journal)
    before = schema_state(journal), table_state(journal)
    with journal._writer():
        with pytest.raises(portalocker.exceptions.LockException):
            MemoryJournal(journal.root, embedder=Embedder(), projection=Projection(), create=True)
    assert (schema_state(journal), table_state(journal)) == before


def test_pending_index_tracks_foreign_scope_and_retired_cleanup(journal):
    mine = journal.store(scope="alice", operation_id="mine", observation=observation(), index=False)
    journal.store(scope="bob", operation_id="foreign", observation=observation(), index=False)
    assert journal.status()["status"] == "unavailable"
    assert journal.sync(scope="alice") == 1
    assert journal.status()["status"] == "unavailable"
    assert journal.sync(scope="bob") == 1
    assert journal.status()["status"] == "ready"

    journal.revise(scope="alice", operation_id="forget", expected_ref=mine["record_ref"])
    assert journal.get(scope="alice", record_ref=mine["record_ref"]) == {"status": "not-found"}
    assert journal.status()["status"] == "unavailable"
    with journal._connect() as db:
        assert (
            db.execute("SELECT count(*) FROM records WHERE retired=1 AND indexed=0").fetchone()[0]
            == 1
        )
    journal.projection.fail = True
    with pytest.raises(TimeoutError):
        journal.sync(scope="alice")
    assert journal.status()["status"] == "unavailable"
    journal.projection.fail = False
    assert journal.sync(scope="alice") == 1
    assert journal.status()["status"] == "ready"


def test_explicit_integrity_checks_whole_database_not_only_meta_or_records(journal):
    # Full maintenance, not lightweight status, must detect unrelated corruption.
    with journal._connect() as db:
        db.execute("CREATE TABLE integrity_probe(value INTEGER CHECK(value > 0))")
        db.execute("PRAGMA ignore_check_constraints=ON")
        db.execute("INSERT INTO integrity_probe VALUES(0)")
    assert journal.status()["integrity"] == "not-checked"
    result = journal.integrity_check()
    assert result["status"] == "failed"
    assert result["observation_scope"] == "whole-database"


def test_unavailable_integrity_check_cannot_be_bypassed(journal, monkeypatch):
    original_connect = sqlite3.connect

    def unavailable_check(*args, **kwargs):
        db = original_connect(*args, **kwargs)
        def authorize(action, name, *_):
            if action == sqlite3.SQLITE_PRAGMA and name.lower() == "quick_check":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        db.set_authorizer(authorize)
        return db

    monkeypatch.setattr(sqlite3, "connect", unavailable_check)
    with pytest.raises(sqlite3.DatabaseError, match="authorized"):
        journal.integrity_check()


def test_prepared_journal_restart_keeps_later_write_and_operation_custody(journal):
    first = journal.store(scope="alice", operation_id="first", observation=observation())
    legacy_without_index(journal)
    prepared = MemoryJournal(
        journal.root, embedder=Embedder(), projection=journal.projection, create=True
    )
    later = prepared.store(
        scope="alice",
        operation_id="later",
        observation=observation("later fixture write"),
        index=False,
    )
    assert prepared.status()["status"] == "unavailable"
    restarted = MemoryJournal(journal.root, embedder=Embedder(), projection=journal.projection)
    assert restarted.get(scope="alice", record_ref=first["record_ref"]) == first
    assert (
        restarted.reconcile(
            scope="alice", operation_id="later", request_digest=later["request_digest"]
        )
        == later
    )
    assert restarted.get(scope="bob", record_ref=later["record_ref"]) == {"status": "not-found"}
    assert restarted.sync(scope="alice") == 1
    assert restarted.status()["status"] == "ready"
    assert (
        restarted.store(
            scope="alice", operation_id="later", observation=observation("later fixture write")
        )
        == later
    )
    with restarted._connect() as db:
        assert db.execute("SELECT count(*) FROM records").fetchone()[0] == 2


@pytest.mark.parametrize("prepared", [True, False])
def test_worker_status_reopens_without_model_or_credentials_or_implicit_preparation(
    tmp_path, prepared
):
    missing = []
    for dependency in ("mempalace", "onnxruntime", "tokenizers", "numpy"):
        try:
            version(dependency)
        except PackageNotFoundError:
            missing.append(dependency)
    if missing:
        pytest.skip("worker IPC requires optional memory extras: " + ", ".join(missing))

    class WorkerIdentityEmbedder(Embedder):
        identity = MODEL_IDENTITY

    journal = MemoryJournal(
        tmp_path / "worker-journal",
        embedder=WorkerIdentityEmbedder(),
        projection=Projection(),
        create=True,
    )
    if not prepared:
        legacy_without_index(journal)
    request = {
        "operation": "status",
        "options": {},
        "config": {
            "root": str(journal.root),
            "scope": "alice",
            "provider_digest": mempalace_identity(),
            "model_identity": MODEL_IDENTITY,
            "model_root": str(tmp_path / "absent-model"),
            "credential_file": str(tmp_path / "absent-credential"),
            "palace_id": "fixture-palace",
            "qdrant_url": "http://127.0.0.1:1",
            "timeout": 1,
        },
    }
    # Real worker IPC; fixture-only database. Forbid all external access even if
    # status regresses and accidentally tries to load the projection.
    script = (
        "import sys\n"
        "def no_network(event, args):\n"
        " if event.startswith('socket.'):\n"
        "  raise RuntimeError('status fixture network forbidden')\n"
        "sys.addaudithook(no_network)\n"
        "from ahc.adapters.mempalace_memory_worker import main\nmain()\n"
    )
    environment = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).parents[1] / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    for expected in ("ready", "unavailable"):
        before = journal.database.read_bytes()
        result = subprocess.run(
            [sys.executable, "-c", script],
            input=json.dumps(request).encode(),
            capture_output=True,
            timeout=15,
            cwd=tmp_path,
            env=environment,
        )
        assert result.returncode == 0, result.stderr.decode()
        value = json.loads(result.stdout)
        assert value["schema_version"] == "ahc.memory.native.status.v3"
        assert value["integrity"] == "not-checked"
        assert value["status"] == expected
        assert journal.database.read_bytes() == before
        assert not (tmp_path / "absent-model").exists()
        assert not (tmp_path / "absent-credential").exists()
        if expected == "ready":
            journal.store(
                scope="alice", operation_id="pending", observation=observation(), index=False
            )
