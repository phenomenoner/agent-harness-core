import json
import sqlite3

import pytest
from test_memory_journal import Embedder, Projection, observation

from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_journal import MemoryJournal
from ahc.infrastructure.memory_migration import MemoryMigration, snapshot_sqlite, sqlite_rows


@pytest.fixture
def migration(tmp_path):
    journal = MemoryJournal(
        tmp_path / "journal", embedder=Embedder(), projection=Projection(), create=True
    )
    return MemoryMigration(journal)


def test_snapshot_includes_wal_blobs_and_never_overwrites(tmp_path):
    source, snapshot = tmp_path / "source.db", tmp_path / "snapshot.db"
    db = sqlite3.connect(source)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY,summary TEXT,detail BLOB)")
    db.execute("INSERT INTO observations VALUES(1,?,?)", ("繁體中文", b"\x00\xff"))
    db.commit()
    result = snapshot_sqlite(source, snapshot)
    assert len(result["sha256"]) == 64
    rows = list(sqlite_rows(snapshot, ["observations"]))
    assert rows[0][1]["row"] == {
        "id": 1,
        "summary": "繁體中文",
        "detail": {"sqlite_blob_base64": "AP8="},
    }
    db.execute("INSERT INTO observations VALUES(2,'new',NULL)")
    db.commit()
    assert len(list(sqlite_rows(snapshot, ["observations"]))) == 1
    with pytest.raises(FileExistsError):
        snapshot_sqlite(source, snapshot)
    with pytest.raises(ContractError, match="unsupported"):
        list(sqlite_rows(source, ["cron_jobs"]))
    db.close()


def test_full_denominator_and_exact_resumption(migration):
    migration.begin("cutoff", {"digest": "source"}, expected=2)
    batch = [("one", {"summary": "original"})]
    assert migration.stage("cutoff", batch) == 1
    assert migration.stage("cutoff", batch) == 1
    with pytest.raises(ContractError, match="incomplete"):
        migration.seal("cutoff")
    with pytest.raises(ContractError, match="changed"):
        migration.stage("cutoff", [("one", {"summary": "changed"})])
    with pytest.raises(ContractError, match="new delta"):
        migration.begin("cutoff", {"digest": "different"}, expected=2)
    with pytest.raises(ContractError, match="denominator"):
        migration.stage("cutoff", [("two", {}), ("three", {})])
    assert migration.reconcile("cutoff")["preserved"] == 1  # Whole batch rolled back.
    migration.stage("cutoff", [("two", {})])
    migration.seal("cutoff")
    assert not migration.reconcile("cutoff")["technical_complete"]
    with pytest.raises(ContractError, match="sealed"):
        migration.stage("cutoff", [("three", {})])


def test_explicit_scope_index_freshness_and_acknowledgment_loss(migration, monkeypatch):
    migration.begin("source", {}, expected=2)
    migration.stage("source", [("one", {"summary": "source bytes"}), ("two", {"type": "log"})])
    migration.seal("source")
    with pytest.raises(ContractError, match="scope"):
        migration.classify(
            "source", "one", disposition="serve", reason="preference", observation=observation()
        )
    migration.classify(
        "source",
        "one",
        disposition="serve",
        scope="alice",
        reason="explicit owner",
        observation=observation(),
    )
    migration.classify("source", "two", disposition="archive", reason="operational history")
    store = migration.journal.store

    def loss(**kwargs):
        store(**kwargs)
        raise TimeoutError("committed before importer response")

    monkeypatch.setattr(migration.journal, "store", loss)
    with pytest.raises(TimeoutError):
        migration.import_one("source", "one")
    monkeypatch.setattr(migration.journal, "store", store)
    saved = migration.import_one("source", "one")
    assert migration.import_one("source", "one") == saved
    assert (
        migration.journal.get(scope="bob", record_ref=saved["record_ref"])["status"] == "not-found"
    )
    assert not migration.reconcile("source")["technical_complete"]
    assert migration.journal.sync(scope="alice") == 1
    assert migration.reconcile("source")["technical_complete"]
    with pytest.raises(ContractError, match="immutable"):
        migration.classify("source", "one", disposition="archive", reason="changed mind")


def test_originals_dispositions_and_new_writes_survive_joint_database_restore(migration, tmp_path):
    migration.begin("source", {}, expected=1)
    original = {"nested": {"text": "old\nfull content", "other": [1, 2]}}
    migration.stage("source", [("one", original)])
    migration.seal("source")
    migration.classify("source", "one", disposition="blocked", reason="owner unresolved")
    backup = tmp_path / "backup"
    migration.journal.backup(backup)
    new = migration.journal.store(
        scope="alice", operation_id="after-backup", observation=observation()
    )
    restored = MemoryJournal.restore(
        backup, tmp_path / "restored", embedder=Embedder(), projection=Projection()
    )
    ledger = MemoryMigration(restored)
    assert ledger.reconcile("source")["dispositions"] == {"blocked": 1}
    assert not ledger.reconcile("source")["technical_complete"]
    with restored._connect() as db:
        assert (
            json.loads(db.execute("SELECT original FROM migration_entries").fetchone()[0])
            == original
        )
    assert (
        migration.journal.get(scope="alice", record_ref=new["record_ref"])["status"] == "succeeded"
    )
    assert restored.get(scope="alice", record_ref=new["record_ref"])["status"] == "not-found"


def test_original_tamper_cannot_be_served(migration):
    migration.begin("source", {}, expected=1)
    migration.stage("source", [("one", {"summary": "original"})])
    migration.seal("source")
    migration.classify(
        "source",
        "one",
        disposition="serve",
        scope="alice",
        reason="explicit owner",
        observation=observation(),
    )
    with migration.journal._connect() as db:
        db.execute("UPDATE migration_entries SET original='{}'")
    with pytest.raises(ContractError, match="integrity"):
        migration.import_one("source", "one")


def test_delta_deduplicates_unchanged_rows_and_never_overwrites_newer_writes(migration):
    def stage(source, text):
        migration.begin(source, {"logical_source": "owned-legacy"}, expected=1)
        migration.stage(source, [("one", {"summary": text})])
        migration.seal(source)
        migration.classify(
            source,
            "one",
            disposition="serve",
            scope="alice",
            reason="explicit owner",
            observation=observation(text),
        )

    stage("cutoff", "old")
    first = migration.import_one("cutoff", "one")
    stage("unchanged", "old")
    assert migration.import_one("unchanged", "one") == first
    stage("changed", "updated source")
    with pytest.raises(ContractError, match="explicit current predecessor"):
        migration.import_one("changed", "one")
    updated = migration.import_one("changed", "one", expected_ref=first["record_ref"])
    assert updated["record_ref"] != first["record_ref"]
    assert migration.import_one("changed", "one", expected_ref=first["record_ref"]) == updated
    stage("next-cutoff", "updated source")
    assert migration.import_one("next-cutoff", "one") == updated
    mutation = migration.journal.revise(
        scope="alice",
        operation_id="new-user-correction",
        expected_ref=updated["record_ref"],
        observation=observation("new user correction"),
    )
    # Final-delta preparation follows a reopen, not merely the in-memory caller
    # that acknowledged the newer write. Keep its migration ledger and scope.
    migration = MemoryMigration(MemoryJournal(
        migration.journal.root, embedder=Embedder(), projection=migration.journal.projection
    ))
    stage("after-user-correction", "updated source")
    with pytest.raises(ContractError, match="later correction"):
        migration.import_one("after-user-correction", "one")
    stage("another-delta", "another source edit")
    with pytest.raises(ContractError, match="predecessor changed"):
        migration.import_one("another-delta", "one", expected_ref=updated["record_ref"])
    assert (
        migration.journal.get(scope="alice", record_ref=mutation["record_ref"])["record"]["summary"]
        == "new user correction"
    )


def test_batch_import_is_atomic_and_resumable_without_embedding(migration):
    migration.begin("source", {}, expected=3)
    migration.stage("source", [(key, {"key": key}) for key in ("a", "b", "c")])
    migration.seal("source")
    decision = {
        "disposition": "serve",
        "reason": "explicit owner",
        "scope": "alice",
        "observation": observation(),
    }
    migration.classify_batch("source", [(key, decision) for key in ("a", "b", "c")])
    first = migration.import_pending("source", limit=2)
    assert len(first) == 2
    assert len(migration.import_pending("source", limit=2)) == 1
    assert migration.import_pending("source", limit=2) == []
    assert migration.journal.projection.items == {}
    assert migration.reconcile("source")["pending_import"] == 0
    assert not migration.reconcile("source")["technical_complete"]
    assert migration.journal.sync(scope="alice") == 3
    assert migration.reconcile("source")["technical_complete"]


def test_bad_batch_record_rolls_back_all_prior_records(migration):
    migration.begin("source", {}, expected=2)
    migration.stage("source", [("a", {}), ("b", {"valid": True})])
    migration.seal("source")
    decision = {
        "disposition": "serve",
        "reason": "explicit owner",
        "scope": "alice",
        "observation": observation(),
    }
    with pytest.raises(ContractError):
        migration.classify_batch("source", [("a", decision), ("b", {**decision, "scope": None})])
    assert migration.reconcile("source")["dispositions"] == {"unresolved": 2}
    migration.classify_batch("source", [("a", decision), ("b", decision)])
    with migration.journal._connect() as db:
        db.execute("UPDATE migration_entries SET original='{}' WHERE source_key='b'")
    with pytest.raises(ContractError, match="integrity"):
        migration.import_pending("source")
    with migration.journal._connect() as db:
        assert db.execute("SELECT count(*) FROM records").fetchone()[0] == 0
    assert migration.reconcile("source")["pending_import"] == 2
