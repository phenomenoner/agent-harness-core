"""Actual SQLite lexical reads with foreign, stale and later-write boundaries."""
import pytest
from test_memory_journal import memory, observation


def build(memory,scope):
    for _ in range(4):
        if not memory.build_keyword_index(scope=scope,limit=2):break
    else:raise AssertionError('fixture build budget')


def test_pending_vectors_do_not_block_complete_scoped_keyword_projection(memory):
    memory.projection.fail=True
    owned=memory.store(scope='main',operation_id='owned',observation=observation('繁體中文使用者知識 alpha'))
    memory.store(scope='other',operation_id='foreign',observation=observation('繁體中文 secret'))
    build(memory,'main')
    assert memory.status()['status']=='unavailable'  # The other scope is still incomplete.
    build(memory,'other')
    assert memory.status()['code']=='native-keyword-ready'
    memory.embedder.embed=lambda *_:(_ for _ in ()).throw(AssertionError('read must not load embedding'))
    assert memory.search('繁體中文',scope='main')==[{'id':1,'summary':'繁體中文使用者知識 alpha'}]
    assert memory.get(scope='main',record_ref=owned['record_ref'])==owned
    later=memory.store(scope='main',operation_id='later',observation=observation('後續新增 beta'))
    assert memory.search('後續新增',scope='main')[0]['summary']=='後續新增 beta'
    assert later['status']=='succeeded'


def test_vector_timeout_falls_back_without_leaking_foreign_or_retired_records(memory):
    old=memory.store(scope='main',operation_id='old',observation=observation('exact phrase old'))
    build(memory,'main')
    memory.projection.query=lambda **_:(_ for _ in ()).throw(TimeoutError('offline'))
    assert memory.search('exact phrase',scope='main')[0]['id']==1
    memory.revise(scope='main',operation_id='retire',expected_ref=old['record_ref'])
    assert memory.search('exact phrase',scope='main')==[]
    memory.store(scope='other',operation_id='foreign',observation=observation('exact phrase secret'))
    assert memory.search('exact phrase',scope='main')==[]


def test_missing_or_partial_keyword_index_does_not_claim_fallback_coverage(memory):
    memory.store(scope='main',operation_id='old',observation=observation())
    memory.projection.query=lambda **_:(_ for _ in ()).throw(TimeoutError('offline'))
    with pytest.raises(TimeoutError):memory.search('Traditional',scope='main')
    assert memory.build_keyword_index(scope='main',limit=1)==1
    with pytest.raises(TimeoutError):memory.search('Traditional',scope='main')


def test_stale_index_text_never_replaces_authoritative_text_and_fts_syntax_is_inert(memory):
    memory.store(scope='main',operation_id='owned',observation=observation('actual retained text'))
    build(memory,'main')
    with memory._writer(),memory._connect() as db:
        db.execute('UPDATE memory_keywords SET summary=? WHERE rowid=1',('stale injected keyword',))
        db.execute('UPDATE records SET indexed=0 WHERE id=1')
    assert memory.search('injected',scope='main')==[{'id':1,'summary':'actual retained text'}]
    assert memory.search('"injected" OR scope:other',scope='main')[0]['summary']=='actual retained text'


def test_keyword_projection_cannot_supply_journal_integrity_or_scope_authority(memory):
    from ahc.domain.sessions import ContractError
    memory.store(scope='main',operation_id='owned',observation=observation('authenticated keyword'))
    build(memory,'main')
    with memory._writer(),memory._connect() as db:
        db.execute("UPDATE records SET observation=?,indexed=0 WHERE id=1",
                   ('{"kind":"note","summary":"forged keyword","detail":{}}',))
    with pytest.raises(ContractError,match='integrity'):
        memory.search('authenticated',scope='main')


def test_pending_corpus_keeps_acknowledged_semantic_reads_without_indexing_on_read(memory):
    owned=memory.store(scope='main',operation_id='cat',observation=observation('貓咪在窗邊曬太陽'))
    memory.store(scope='other',operation_id='foreign',observation=observation('foreign secret'))
    memory.projection.fail=True
    memory.store(scope='main',operation_id='pending',observation=observation('unacknowledged point'))
    build(memory,'main');build(memory,'other')
    assert memory.status()['code']=='native-keyword-ready'
    memory.sync=lambda **_:(_ for _ in ()).throw(AssertionError('partial search must not index'))
    hits=memory.search('Cats bask beside a window',scope='main')
    assert hits==[{'id':1,'summary':owned['record']['summary']}]
    memory.revise(scope='main',operation_id='retire',expected_ref=owned['record_ref'])
    assert memory.search('Cats bask beside a window',scope='main')==[]
