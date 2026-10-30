"""Stored-vector batches preserve committed originals and pending retry authority."""
import json
from types import SimpleNamespace

import pytest
from test_memory_journal import Embedder, Projection, observation

from ahc.adapters.mempalace_projection import MemPalaceProjection
from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_journal import MemoryJournal


class BatchProjection(Projection):
    def __init__(self):
        super().__init__()
        self.batches = []
        self.fail_batch = None

    def upsert_many(self, items):
        self.batches.append([dict(item) for item in items])
        for item in items:
            self.items[item['identifier']] = item
        if len(self.batches) == self.fail_batch:
            raise TimeoutError('derived write happened, reply lost')


def seeded(tmp_path):
    projection = BatchProjection()
    journal = MemoryJournal(tmp_path / 'journal', embedder=Embedder(), projection=projection, create=True)
    receipts = [journal.store(scope='alice', operation_id=str(i), observation=observation(str(i))) for i in range(2)]
    foreign = journal.store(scope='bob', operation_id='foreign', observation=observation('private'))
    with journal._connect() as db:
        db.execute('UPDATE records SET indexed=0')
    projection.items.clear()
    return journal, projection, receipts, foreign


def pending(journal):
    with journal._connect() as db:
        return {row['scope']: row['n'] for row in db.execute('SELECT scope,count(*) n FROM records WHERE indexed=0 GROUP BY scope')}


def test_batch_success_preserves_originals_scope_reopen_and_no_reembedding(tmp_path):
    journal, projection, receipts, foreign = seeded(tmp_path)
    journal.embedder.embed = lambda _: pytest.fail('stored vectors must not re-embed')
    assert journal.sync(scope='alice', limit=64) == 2
    assert len(projection.batches) == 1
    assert {item['scope'] for item in projection.batches[0]} == {'alice'}
    assert pending(journal) == {'bob': 1}
    reopened = MemoryJournal(journal.root, embedder=Embedder(), projection=projection)
    for receipt in receipts:
        assert reopened.get(scope='alice', record_ref=receipt['record_ref']) == receipt
    assert reopened.get(scope='alice', record_ref=foreign['record_ref']) == {'status': 'not-found'}
    assert reopened.sync(scope='alice') == 0


def test_second_batch_lost_reply_keeps_all_pending_then_retry_is_derived_only(tmp_path):
    journal, projection, receipts, _ = seeded(tmp_path)
    with journal._connect() as db:
        row = db.execute('SELECT * FROM chunks WHERE record_ref=?', (receipts[0]['record_ref'],)).fetchone()
        db.executemany('INSERT INTO chunks VALUES(?,?,?,?,?)', [
            (row['record_ref'] + ':' + str(i), row['record_ref'], row['content'], row['content_digest'], row['vector']) for i in range(1, 64)
        ])
    projection.fail_batch = 2
    with pytest.raises(TimeoutError, match='reply lost'):
        journal.sync(scope='alice', limit=64)
    assert [len(batch) for batch in projection.batches] == [64, 1]
    assert pending(journal) == {'alice': 2, 'bob': 1}
    projection.fail_batch = None
    reopened = MemoryJournal(journal.root, embedder=Embedder(), projection=projection)
    assert reopened.sync(scope='alice', limit=64) == 2
    assert len(projection.items) == 65
    assert pending(reopened) == {'bob': 1}
    for receipt in receipts:
        assert reopened.get(scope='alice', record_ref=receipt['record_ref']) == receipt


@pytest.mark.parametrize('kind', ['original', 'chunk'])
def test_tampering_rejected_before_external_effect(tmp_path, kind):
    journal, projection, _, _ = seeded(tmp_path)
    with journal._connect() as db:
        if kind == 'original':
            db.execute("UPDATE records SET observation=? WHERE scope='alice'", (json.dumps(observation('tampered')),))
        else:
            db.execute("UPDATE chunks SET content='tampered'")
    with pytest.raises(ContractError, match='integrity'):
        journal.sync(scope='alice', limit=64)
    assert projection.batches == []
    assert pending(journal) == {'alice': 2, 'bob': 1}


@pytest.mark.parametrize('kind', ['fresh', 'retired', 'oversized'])
def test_ineligible_batch_uses_original_path(tmp_path, kind):
    journal, projection, receipts, _ = seeded(tmp_path)
    with journal._connect() as db:
        if kind == 'fresh':
            db.execute('DELETE FROM chunks WHERE record_ref=?', (receipts[0]['record_ref'],))
        elif kind == 'retired':
            db.execute('UPDATE records SET retired=1 WHERE ref=?', (receipts[0]['record_ref'],))
        else:
            row = db.execute('SELECT * FROM chunks WHERE record_ref=?', (receipts[0]['record_ref'],)).fetchone()
            db.executemany('INSERT INTO chunks VALUES(?,?,?,?,?)', [
                (row['record_ref'] + ':' + str(i), row['record_ref'], row['content'], row['content_digest'], row['vector']) for i in range(1, 257)
            ])
    assert journal.sync(scope='alice', limit=64) == 2
    assert projection.batches == []
    assert pending(journal) == {'bob': 1}
    if kind == 'retired':
        assert journal.get(scope='alice', record_ref=receipts[0]['record_ref']) == {'status': 'not-found'}


class Collection:
    def __init__(self, mutation=None):
        self.calls = []
        self.mutation = mutation

    def upsert(self, **kwargs):
        self.calls.append(kwargs)
        self.value = kwargs

    def get(self, **kwargs):
        assert kwargs['where'] == {'scope': 'alice'}
        result = SimpleNamespace(ids=self.value['ids'][::-1], documents=self.value['documents'][::-1],
                                 metadatas=self.value['metadatas'][::-1])
        if self.mutation:
            self.mutation(result)
        return result


def point(identifier='one', scope='alice'):
    return dict(identifier=identifier, scope=scope, text=identifier, vector=[1., 0.], content_digest='digest-' + identifier)


def adapter(collection):
    value = MemPalaceProjection.__new__(MemPalaceProjection)
    value.collection = collection
    return value


def test_projection_accepts_reordered_exact_readback():
    collection = Collection()
    adapter(collection).upsert_many([point(), point('two')])
    assert len(collection.calls) == 1


@pytest.mark.parametrize('mutation', [
    lambda result: result.ids.pop(),
    lambda result: result.ids.__setitem__(0, result.ids[1]),
    lambda result: result.ids.__setitem__(0, 'foreign-id'),
    lambda result: result.metadatas[0].__setitem__('scope', 'bob'),
    lambda result: result.metadatas[0].__setitem__('content_digest', 'wrong'),
    lambda result: result.documents.__setitem__(0, 'wrong'),
    lambda result: result.metadatas.pop(),
])
def test_projection_rejects_incomplete_duplicate_foreign_or_changed_readback(mutation):
    collection = Collection(mutation)
    with pytest.raises(ContractError, match='readback mismatch'):
        adapter(collection).upsert_many([point(), point('two')])
    assert len(collection.calls) == 1


@pytest.mark.parametrize('items', [[], [point()] * 65, [point(), point()], [point(), point('two', 'bob')]])
def test_projection_rejects_invalid_batch_before_write(items):
    collection = Collection()
    with pytest.raises(ContractError):
        adapter(collection).upsert_many(items)
    assert collection.calls == []
