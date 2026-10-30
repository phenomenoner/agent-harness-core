"""Shared vectors save inference, never merge originals or cross owner scope."""

import pytest
from test_memory_journal import Embedder, Projection, observation

from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_indexing import semantic_text
from ahc.infrastructure.memory_journal import MemoryJournal


def test_semantic_text_retains_facts_without_transport_coordinates():
    value = observation()
    value["detail"] = {
        "migration_key": "opaque",
        "tool_call_id": "call",
        "session_key": "route",
        "timezone": "Asia/Taipei",
        "exception": "Use English for exported APIs",
    }
    text = semantic_text(value)
    assert "opaque" not in text and "tool_call_id" not in text
    assert "Asia/Taipei" in text and "exported APIs" in text
    assert value["detail"]["migration_key"] == "opaque"


def test_equal_content_reuses_vectors_but_preserves_each_original_and_scope(tmp_path):
    calls = []

    class CountingEmbedding(Embedder):
        def embed(self, texts):
            calls.extend(texts)
            return super().embed(texts)

    journal = MemoryJournal(
        tmp_path / "journal", embedder=CountingEmbedding(), projection=Projection(), create=True
    )
    one = journal.store(scope="alice", operation_id="one", observation=observation())
    changed_trace = observation()
    changed_trace["detail"] = {"tool_call_id": "other-call", "legacy_timestamp": "yesterday"}
    two = journal.store(scope="alice", operation_id="two", observation=changed_trace)
    assert len(calls) == 1
    assert one["record_ref"] != two["record_ref"]
    assert (
        journal.get(scope="alice", record_ref=two["record_ref"])["record"]["detail"]["tool_call_id"]
        == "other-call"
    )
    journal.store(scope="bob", operation_id="other-owner", observation=observation())
    assert len(calls) == 2
    backup = tmp_path / "backup"
    journal.backup(backup)
    restored = MemoryJournal.restore(
        backup, tmp_path / "restored", embedder=CountingEmbedding(), projection=Projection()
    )
    restored.sync(scope="alice")
    restored.store(scope="alice", operation_id="after-restore", observation=observation())
    assert len(calls) == 2


def test_unknown_index_policy_is_not_silently_reused(tmp_path):
    journal = MemoryJournal(
        tmp_path / "journal", embedder=Embedder(), projection=Projection(), create=True
    )
    original = journal.store(scope="alice", operation_id="one", observation=observation())
    with journal._connect() as db:
        db.execute("DELETE FROM meta WHERE key='index_policy'")
    assert journal.status()["status"] == "unavailable"
    with pytest.raises(ContractError, match="explicit projection rebuild"):
        journal.search("language", scope="alice")
    assert journal.get(scope="alice", record_ref=original["record_ref"]) == original


def test_cache_corruption_does_not_publish_a_false_vector(tmp_path):
    journal = MemoryJournal(
        tmp_path / "journal", embedder=Embedder(), projection=Projection(), create=True
    )
    journal.store(scope="alice", operation_id="one", observation=observation())
    with journal._connect() as db:
        db.execute("UPDATE embedding_cache SET vector='[0,1]'")
    result = journal.store(scope="alice", operation_id="two", observation=observation())
    assert result["status"] == "succeeded"
    assert journal.status()["status"] == "unavailable"
    with pytest.raises(ContractError, match="integrity changed"):
        journal.sync(scope="alice")
    assert len(journal.projection.items) == 1


def test_existing_journal_preparation_bounds_recovery_lookups_without_changing_rows():
    """An old full corpus must not scan every chunk for each recovered record."""
    import sqlite3

    from ahc.infrastructure.memory_indexing import INDEX_POLICY, prepare
    from ahc.infrastructure.memory_journal import _SCHEMA

    with sqlite3.connect(":memory:") as db:
        db.executescript(_SCHEMA)
        db.execute("INSERT INTO meta VALUES('index_policy', ?)", (INDEX_POLICY,))
        db.executemany(
            "INSERT INTO records(id,ref,scope,operation_id,request_digest,observation,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            ((i, f"ref-{i:05}", "alice", str(i), "digest", "{}", "test") for i in range(2000)),
        )
        db.executemany(
            "INSERT INTO chunks VALUES(?,?,?,?,?)",
            ((f"chunk-{i:05}", f"ref-{i:05}", "original", "digest", "[1.0]")
             for i in range(2000)),
        )
        db.execute("UPDATE records SET indexed=1 WHERE id<1999")
        before = {table: db.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
                  for table in ("records", "chunks", "meta")}
        prepare(db)
        prepare(db)  # Idempotent on existing journals, not only new schemas.
        after = {table: db.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
                 for table in before}
        assert after == before
        queries = (
            ("SELECT * FROM chunks WHERE record_ref=? ORDER BY id", ("ref-01999",),
             "chunks_record_ref_id"),
            ("SELECT * FROM records WHERE scope=? AND indexed=0 ORDER BY id LIMIT ?",
             ("alice", 64), "records_scope_indexed_id"),
        )
        for sql, params, index_name in queries:
            plan = " ".join(row[3] for row in db.execute("EXPLAIN QUERY PLAN " + sql, params))
            assert "SEARCH" in plan and index_name in plan, plan
            assert "TEMP B-TREE" not in plan, plan
            steps = []
            db.set_progress_handler(lambda: steps.append(1) or 0, 1)
            rows = db.execute(sql, params).fetchall()
            db.set_progress_handler(None, 0)
            assert len(rows) == 1
            assert len(steps) < 200, (index_name, len(steps))


def test_unknown_policy_does_not_install_recovery_indexes():
    import sqlite3

    from ahc.infrastructure.memory_indexing import prepare
    from ahc.infrastructure.memory_journal import _SCHEMA

    with sqlite3.connect(":memory:") as db:
        db.executescript(_SCHEMA)
        db.execute("INSERT INTO meta VALUES('index_policy','foreign-policy')")
        before = db.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall()
        with pytest.raises(ContractError, match="explicit projection rebuild"):
            prepare(db)
        assert db.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall() == before
