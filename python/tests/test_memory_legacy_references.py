"""Old citations and Store acknowledgments survive migration without replay."""

from copy import deepcopy

import pytest
from test_memory_journal import Embedder, Projection, observation

from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_journal import MemoryJournal
from ahc.infrastructure.memory_legacy_references import adopt_namespace


def prepare(tmp_path):
    journal = MemoryJournal(
        tmp_path / "journal", embedder=Embedder(), projection=Projection(), create=True
    )
    obs = observation()
    record = journal.store(
        scope="alice", operation_id="import:source", observation=obs, index=False
    )
    old = deepcopy(record)
    old["record_ref"] = "memory:" + "a" * 32 + ":" + "c" * 32
    old["trace"]["operation_id"] = "old-store"
    citation = deepcopy(old)
    citation["record_ref"] = "citation:" + "a" * 32 + ":42:" + "b" * 64
    citation["trace"]["operation_id"] = "citation:42"
    entries = [
        {
            "ref": record["record_ref"],
            "row_id": 42,
            "scope": "alice",
            "results": [citation, old],
            "operation_ref": old["record_ref"],
        }
    ]
    return journal, record, old, citation, entries


def test_numeric_refs_exact_results_and_operation_retry_preserved(tmp_path):
    journal, record, old, citation, entries = prepare(tmp_path)
    result = adopt_namespace(journal, source_id="snapshot", entries=entries)
    assert adopt_namespace(journal, source_id="snapshot", entries=entries) == result
    assert journal.resolve(scope="alice", row_id=42)["record_ref"] == record["record_ref"]
    assert journal.resolve(scope="alice", row_id=1) == {"status": "not-found"}
    for alias in (old, citation):
        assert journal.get(scope="alice", record_ref=alias["record_ref"]) == alias
        assert journal.get(scope="bob", record_ref=alias["record_ref"]) == {"status": "not-found"}
    assert journal.store(scope="alice", operation_id="old-store", observation=observation()) == old
    assert (
        journal.reconcile(
            scope="alice", operation_id="old-store", request_digest=old["request_digest"]
        )
        == old
    )
    with pytest.raises(ContractError, match="payload changed"):
        journal.store(scope="alice", operation_id="old-store", observation=observation("different"))
    with journal._connect() as db:
        assert db.execute("SELECT count(*) FROM records").fetchone()[0] == 1
    new = journal.store(
        scope="alice", operation_id="later", observation=observation("later"), index=False
    )
    assert journal.resolve(scope="alice", row_id=43)["record_ref"] == new["record_ref"]


def test_old_ref_correction_does_not_follow_successor_or_resurrect(tmp_path):
    journal, record, old, citation, entries = prepare(tmp_path)
    adopt_namespace(journal, source_id="snapshot", entries=entries)
    corrected = journal.revise(
        scope="alice",
        operation_id="correct",
        expected_ref=citation["record_ref"],
        observation=observation("new preference"),
    )
    assert corrected["record_ref"] != record["record_ref"]
    for alias in (old, citation, record):
        assert journal.get(scope="alice", record_ref=alias["record_ref"]) == {"status": "not-found"}
    with pytest.raises(ContractError, match="predecessor"):
        journal.revise(scope="alice", operation_id="stale", expected_ref=old["record_ref"])
    assert journal.store(scope="alice", operation_id="old-store", observation=observation()) == old
    assert journal.get(scope="alice", record_ref=old["record_ref"]) == {"status": "not-found"}
    with pytest.raises(ContractError, match="identity already"):
        journal.revise(
            scope="alice", operation_id="old-store", expected_ref=corrected["record_ref"]
        )


@pytest.mark.parametrize("change", ["new-write", "indexed", "partial", "foreign", "wrong-content"])
def test_adoption_rejects_unsafe_or_incomplete_map_atomically(tmp_path, change):
    journal, record, old, citation, entries = prepare(tmp_path)
    if change == "new-write":
        journal.store(scope="alice", operation_id="user", observation=observation(), index=False)
    elif change == "indexed":
        journal.sync(scope="alice")
    elif change == "partial":
        journal.store(
            scope="alice", operation_id="import:other", observation=observation(), index=False
        )
    elif change == "foreign":
        entries[0]["scope"] = "bob"
    else:
        entries[0]["results"][0]["record"]["summary"] = "not this record"
    with pytest.raises(ContractError):
        adopt_namespace(journal, source_id="snapshot", entries=entries)
    assert journal.resolve(scope="alice", row_id=1)["record_ref"] == record["record_ref"]
    assert journal.get(scope="alice", record_ref=old["record_ref"]) == {"status": "not-found"}


def test_backup_restores_aliases_and_preserves_later_source_writes(tmp_path):
    journal, record, old, citation, entries = prepare(tmp_path)
    adopt_namespace(journal, source_id="snapshot", entries=entries)
    journal.backup(tmp_path / "backup")
    newer = journal.store(
        scope="alice", operation_id="later", observation=observation("later"), index=False
    )
    restored = MemoryJournal.restore(
        tmp_path / "backup", tmp_path / "restored", embedder=Embedder(), projection=Projection()
    )
    assert restored.get(scope="alice", record_ref=old["record_ref"]) == old
    assert restored.resolve(scope="alice", row_id=42)["record_ref"] == record["record_ref"]
    assert (
        restored.reconcile(
            scope="alice", operation_id="old-store", request_digest=old["request_digest"]
        )
        == old
    )
    assert restored.get(scope="alice", record_ref=newer["record_ref"]) == {"status": "not-found"}
    assert journal.get(scope="alice", record_ref=newer["record_ref"]) == newer


def test_reading_old_journal_does_not_initialize_reference_tables(tmp_path):
    journal, *_ = prepare(tmp_path)
    with journal._connect() as db:
        db.execute("DROP TABLE legacy_memory_operations")
        db.execute("DROP TABLE legacy_memory_refs")
        db.execute("DROP TABLE legacy_memory_namespace")
    opened = MemoryJournal(journal.root, embedder=Embedder(), projection=Projection())
    assert opened.get(scope="alice", record_ref="unknown") == {"status": "not-found"}
    assert opened.reconcile(scope="alice", operation_id="unknown", request_digest="0" * 64) == {
        "status": "unknown"
    }
    with opened._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='legacy_memory_namespace'"
        ).fetchone()


def test_corrupted_alias_binding_or_acknowledgment_cannot_be_reused(tmp_path):
    journal, _, old, _, entries = prepare(tmp_path)
    adopt_namespace(journal, source_id="snapshot", entries=entries)
    newer = journal.store(scope="alice", operation_id="new", observation=observation(), index=False)
    with journal._connect() as db:
        db.execute(
            "UPDATE legacy_memory_refs SET canonical_ref=? WHERE ref=?",
            (newer["record_ref"], old["record_ref"]),
        )
    with pytest.raises(ContractError, match="integrity changed"):
        journal.get(scope="alice", record_ref=old["record_ref"])
    with pytest.raises(ContractError, match="integrity changed"):
        journal.revise(scope="alice", operation_id="forged", expected_ref=old["record_ref"])
    assert journal.get(scope="alice", record_ref=newer["record_ref"]) == newer
