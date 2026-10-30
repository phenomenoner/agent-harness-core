"""Journal custody failures; vector usefulness is tested separately on real ONNX/Qdrant."""

import json
import sqlite3

import pytest

from ahc.domain.sessions import ContractError
from ahc.domain.memory_pack_response import validate_pack_response
from ahc.infrastructure.memory_journal import MemoryJournal


class Embedder:
    identity = "fixture-embedding"

    def embed(self, texts):
        return [[1.0, 0.0] for _ in texts]


class Projection:
    def __init__(self):
        self.items = {}
        self.fail = False

    def upsert(self, **value):
        self.items[value["identifier"]] = value
        if self.fail:
            raise TimeoutError("write happened, response lost")

    def delete(self, *, identifier, scope):
        if self.fail:
            raise TimeoutError()
        item = self.items.get(identifier)
        if item and item["scope"] == scope:
            del self.items[identifier]

    def query(self, *, scope, vector, limit):
        # Deliberately includes foreign/stale rows to check read-side authority.
        return [
            {"id": key, "content_digest": value["content_digest"]}
            for key, value in self.items.items()
        ][:limit]


def observation(summary="Use Traditional Chinese"):
    return {"kind": "preference", "summary": summary, "detail": {}}


@pytest.fixture
def memory(tmp_path):
    return MemoryJournal(
        tmp_path / "journal", embedder=Embedder(), projection=Projection(), create=True
    )


def test_projection_timeout_does_not_lose_original_or_duplicate_operation(memory):
    memory.projection.fail = True
    first = memory.store(scope="alice", operation_id="write", observation=observation())
    assert first["status"] == "succeeded"
    assert memory.status()["status"] == "unavailable"
    assert memory.get(scope="alice", record_ref=first["record_ref"]) == first
    assert (
        memory.reconcile(
            scope="alice", operation_id="write", request_digest=first["request_digest"]
        )
        == first
    )
    again = memory.store(scope="alice", operation_id="write", observation=observation())
    assert again == first
    with pytest.raises(ContractError, match="payload changed"):
        memory.store(scope="alice", operation_id="write", observation=observation("different"))
    memory.projection.fail = False
    assert memory.sync(scope="alice") == 1
    assert memory.status()["status"] == "ready"
    assert len(memory.search("language", scope="alice")) == 1


def test_scope_before_read_and_recheck_after_index(memory):
    private = memory.store(scope="bob", operation_id="private", observation=observation("secret"))
    owned = memory.store(scope="alice", operation_id="mine", observation=observation())
    assert memory.get(scope="alice", record_ref=private["record_ref"]) == {"status": "not-found"}
    assert memory.resolve(scope="alice", row_id=1) == {"status": "not-found"}
    found = memory.search("anything", scope="alice")
    assert [row["summary"] for row in found] == [owned["record"]["summary"]]
    with pytest.raises(ContractError):
        memory.store(
            scope="alice",
            operation_id="forged",
            observation={**observation(), "detail": {"scope": "bob"}},
        )


def test_correction_cas_and_tombstone_filter_stale_projection(memory):
    old = memory.store(scope="alice", operation_id="old", observation=observation())
    result = memory.revise(
        scope="alice",
        operation_id="correct",
        expected_ref=old["record_ref"],
        observation=observation("Prefer Japanese for this project"),
    )
    assert memory.get(scope="alice", record_ref=old["record_ref"]) == {"status": "not-found"}
    assert (
        memory.revise(
            scope="alice",
            operation_id="correct",
            expected_ref=old["record_ref"],
            observation=observation("Prefer Japanese for this project"),
        )
        == result
    )
    with pytest.raises(ContractError, match="predecessor"):
        memory.revise(scope="alice", operation_id="stale", expected_ref=old["record_ref"])
    memory.sync(scope="alice")
    memory.revise(scope="alice", operation_id="forget", expected_ref=result["record_ref"])
    memory.projection.fail = True
    with pytest.raises(TimeoutError):
        memory.search("anything", scope="alice")
    assert memory.get(scope="alice", record_ref=result["record_ref"]) == {"status": "not-found"}
    memory.projection.fail = False
    assert memory.search("anything", scope="alice") == []


def test_embedding_interruption_cannot_permanently_drop_later_chunks(memory):
    calls = 0

    def interrupted(texts):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise TimeoutError()
        return [[1.0, 0.0] for _ in texts]

    memory.embedder.embed = interrupted
    memory.store(scope="alice", operation_id="long", observation=observation("x" * 5000))
    assert memory.status()["status"] == "unavailable"
    memory.embedder.embed = Embedder().embed
    memory.sync(scope="alice")
    assert len(memory.projection.items) == 4
    assert memory.status()["status"] == "ready"


def test_chunker_cannot_silently_drop_the_end_of_a_record(memory):
    memory.embedder.split_text = lambda text: [text[:20]]
    result = memory.store(
        scope="alice", operation_id="long-text", observation=observation("x" * 5000)
    )
    assert result["status"] == "succeeded"
    assert memory.status()["status"] == "unavailable"
    assert memory.projection.items == {}
    assert memory.get(scope="alice", record_ref=result["record_ref"]) == result
    del memory.embedder.split_text
    assert memory.sync(scope="alice") == 1
    assert memory.status()["status"] == "ready"


def test_full_backup_restore_rebuild_and_new_write_custody(memory, tmp_path):
    original = memory.store(scope="alice", operation_id="one", observation=observation())
    backup = tmp_path / "backup"
    memory.backup(backup)
    newer = memory.store(scope="alice", operation_id="two", observation=observation("new data"))
    restored = MemoryJournal.restore(
        backup, tmp_path / "restored", embedder=Embedder(), projection=Projection()
    )
    assert restored.status()["status"] == "unavailable"
    assert restored.get(scope="alice", record_ref=original["record_ref"]) == original
    assert restored.get(scope="alice", record_ref=newer["record_ref"]) == {"status": "not-found"}
    restored.sync(scope="alice")
    assert restored.status()["status"] == "ready"
    assert len(restored.search("language", scope="alice")) == 1
    with pytest.raises(ContractError, match="target"):
        MemoryJournal.restore(backup, memory.root, embedder=Embedder(), projection=Projection())
    assert memory.get(scope="alice", record_ref=newer["record_ref"]) == newer


def test_tampered_record_and_backup_are_not_accepted(memory, tmp_path):
    original = memory.store(scope="alice", operation_id="one", observation=observation())
    with sqlite3.connect(memory.database) as db:
        db.execute("UPDATE records SET observation=?", (json.dumps(observation("forged")),))
    with pytest.raises(ContractError, match="integrity"):
        memory.get(scope="alice", record_ref=original["record_ref"])
    backup = tmp_path / "backup"
    memory.backup(backup)
    with (backup / "memory.db").open("r+b") as stream:
        stream.write(b"tamper")
    with pytest.raises(ContractError, match="digest"):
        MemoryJournal.restore(
            backup, tmp_path / "restored", embedder=Embedder(), projection=Projection()
        )


def test_pack_uses_real_refs_untrusted_data_and_conservative_unicode_budget(memory):
    memory.store(scope="alice", operation_id="one", observation=observation("繁體😀" * 600))
    value = memory.pack(query="language", scope="alice", budget_tokens=300, limit=2)
    pack = validate_pack_response(
        value, query="language", scope="alice", budget_tokens=300, max_items=2
    ).to_dict()
    assert pack["items"][0]["trust"] == "untrusted"
    assert len(pack["bundle_text"].encode()) <= 300
    assert (
        memory.get(scope="alice", record_ref=pack["items"][0]["recordRef"])["status"] == "succeeded"
    )
    value["context_pack"]["items"][0]["trust"] = "system"
    with pytest.raises(ContractError):
        validate_pack_response(
            value, query="language", scope="alice", budget_tokens=300, max_items=2
        )
