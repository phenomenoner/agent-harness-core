"""Real journal/adapter composition with a controlled collection boundary."""
from types import SimpleNamespace

import pytest
from test_memory_journal import Embedder, Projection, observation

from ahc.adapters.mempalace_projection import MemPalaceProjection
from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_journal import MemoryJournal


class CollectionBoundary:
    def __init__(self, journal, expected_pending, fail_read=None):
        self.journal = journal
        self.expected_pending = expected_pending
        self.fail_read = fail_read
        self.points = {}
        self.writes = []
        self.reads = 0

    def upsert(self, *, ids, documents, embeddings, metadatas):
        self.writes.append(len(ids))
        for key, document, vector, metadata in zip(ids, documents, embeddings, metadatas, strict=True):
            self.points[key] = (document, list(vector), dict(metadata))

    def get(self, *, ids, where, include):
        self.reads += 1
        assert where == {'scope': 'alice'}
        with self.journal._connect() as db:
            # No early indexed flag even after a previous external batch succeeded.
            assert db.execute("SELECT count(*) FROM records WHERE scope='alice' AND indexed=0").fetchone()[0] == self.expected_pending
            assert db.execute('PRAGMA synchronous').fetchone()[0] == 2
        keys = list(reversed(ids))
        result = SimpleNamespace(ids=keys, documents=[self.points[key][0] for key in keys],
                                 metadatas=[dict(self.points[key][2]) for key in keys])
        if self.reads == self.fail_read:
            result.metadatas[0]['scope'] = 'foreign'
        return result


def attach(journal, expected_pending, fail_read=None):
    collection = CollectionBoundary(journal, expected_pending, fail_read)
    adapter = MemPalaceProjection.__new__(MemPalaceProjection)
    adapter.collection = collection
    journal.projection = adapter
    return collection


def seed(tmp_path, count, text):
    journal = MemoryJournal(tmp_path / 'journal', embedder=Embedder(), projection=Projection(), create=True)
    receipts = [journal.store(scope='alice', operation_id=str(i), observation=observation(text)) for i in range(count)]
    with journal._connect() as db:
        db.execute("UPDATE records SET indexed=0 WHERE scope='alice'")
    return journal, receipts


def test_exact_64_records_256_chunks_success_through_real_adapter(tmp_path):
    journal, receipts = seed(tmp_path, 64, 'x' * 5000)
    with journal._connect() as db:
        assert db.execute('SELECT count(*) FROM chunks').fetchone()[0] == 256
    collection = attach(journal, 64)
    assert journal.sync(scope='alice', limit=64) == 64
    assert collection.writes == [64, 64, 64, 64]
    assert collection.reads == 4 and len(collection.points) == 256
    with journal._connect() as db:
        assert db.execute('SELECT count(*) FROM records WHERE indexed=0').fetchone()[0] == 0
    for receipt in receipts:
        assert journal.get(scope='alice', record_ref=receipt['record_ref']) == receipt


def test_actual_adapter_readback_refusal_keeps_batch_pending_across_reopen(tmp_path):
    journal, receipts = seed(tmp_path, 2, 'small')
    with journal._connect() as db:
        chunk = db.execute('SELECT * FROM chunks WHERE record_ref=?', (receipts[0]['record_ref'],)).fetchone()
        db.executemany('INSERT INTO chunks VALUES(?,?,?,?,?)', [
            (chunk['record_ref'] + ':' + str(i), chunk['record_ref'], chunk['content'], chunk['content_digest'], chunk['vector'])
            for i in range(1, 64)
        ])
    collection = attach(journal, 2, fail_read=2)
    with pytest.raises(ContractError, match='readback mismatch'):
        journal.sync(scope='alice', limit=64)
    assert collection.writes == [64, 1] and len(collection.points) == 65
    with journal._connect() as db:
        assert db.execute('SELECT count(*) FROM records WHERE indexed=0').fetchone()[0] == 2
    reopened = MemoryJournal(journal.root, embedder=Embedder(), projection=journal.projection)
    collection.journal = reopened
    collection.fail_read = None
    assert reopened.sync(scope='alice', limit=64) == 2
    assert collection.writes == [64, 1, 64, 1] and len(collection.points) == 65
    assert reopened.status()['status'] == 'ready'
    for receipt in receipts:
        assert reopened.get(scope='alice', record_ref=receipt['record_ref']) == receipt
