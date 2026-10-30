"""Operational readiness is not whole-store integrity; no live/model effects."""

import json
import sqlite3
import subprocess
from contextlib import contextmanager
from pathlib import Path

import pytest
from test_memory_journal import Embedder, Projection
from test_memory_observe_service import service, session
from test_mempalace_binding import document, parse

import ahc.adapters.mempalace_memory_process as process_module
import ahc.infrastructure.memory_journal as journal_module
from ahc.adapters.local_embedding import MODEL_IDENTITY
from ahc.adapters.memory_native_process import NativeMemoryNotDispatched, NativeMemoryUnavailable
from ahc.adapters.mempalace_identity import mempalace_identity
from ahc.application.memory_observe_service import MemoryObserveUnavailable
from ahc.cli.mempalace_memory_binding import MemPalaceMemoryBinding
from ahc.infrastructure.memory_journal import MemoryJournal


@pytest.fixture
def journal(tmp_path):
    return MemoryJournal(tmp_path / 'journal', embedder=Embedder(), projection=Projection(),
                         create=True)


def test_operational_status_never_checks_whole_database(journal, monkeypatch):
    connect = journal._connect
    statements = []

    @contextmanager
    def confined():
        with connect() as db:
            db.set_trace_callback(statements.append)
            db.set_authorizer(lambda action, name, *_: sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_PRAGMA and name.lower() == 'quick_check'
                else sqlite3.SQLITE_OK)
            yield db

    monkeypatch.setattr(journal, '_connect', confined)
    before = journal.database.read_bytes()
    value = journal.status()
    assert value['schema_version'] == 'ahc.memory.native.status.v3'
    assert value['status'] == 'ready'
    assert value['observation_scope'] == 'operational'
    assert value['integrity'] == 'not-checked'
    assert not any('quick_check' in sql.lower() for sql in statements)
    assert journal.database.read_bytes() == before


def integrity_result(status):
    return {'schema_version': 'ahc.memory.native.integrity.v1', 'status': status,
            'check': 'sqlite-quick-check', 'observation_scope': 'whole-database'}


def test_explicit_whole_check_detects_violation_outside_operational_tables(journal):
    with journal._connect() as db:
        db.execute('CREATE TABLE integrity_probe(value INTEGER CHECK(value > 0))')
        db.execute('PRAGMA ignore_check_constraints=ON')
        db.execute('INSERT INTO integrity_probe VALUES(0)')
    before = journal.database.read_bytes()
    status = journal.status()
    assert status['status'] == 'ready' and status['integrity'] == 'not-checked'
    assert journal.integrity_check() == integrity_result('failed')
    assert journal.database.read_bytes() == before


def test_integrity_success_is_separate_read_only_and_whole_store(journal, monkeypatch):
    connect = sqlite3.connect
    observations = []
    statements = []

    def inspected(*args, **kwargs):
        db = connect(*args, **kwargs)
        db.set_trace_callback(statements.append)
        observations.append((args, kwargs, db))
        return db

    monkeypatch.setattr(journal_module.sqlite3, 'connect', inspected)
    before = journal.database.read_bytes()
    assert journal.integrity_check() == integrity_result('ok')
    assert len(observations) == 1
    args, kwargs, db = observations[0]
    assert args[0].endswith('?mode=rw') and kwargs['uri'] is True
    assert statements == ['PRAGMA query_only=ON', 'PRAGMA quick_check']
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        db.execute('SELECT 1')
    assert journal.database.read_bytes() == before


def test_denied_integrity_check_is_not_success(journal, monkeypatch):
    connect = sqlite3.connect

    def denied(*args, **kwargs):
        db = connect(*args, **kwargs)
        db.set_authorizer(lambda action, name, *_: sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_PRAGMA and name.lower() == 'quick_check'
            else sqlite3.SQLITE_OK)
        return db

    monkeypatch.setattr(journal_module.sqlite3, 'connect', denied)
    with pytest.raises(sqlite3.DatabaseError, match='authorized'):
        journal.integrity_check()


@pytest.mark.parametrize('schema', ['openclaw-mem.native.status.v1',
                                  'ahc.memory.native.status.v2', 'ahc.memory.native.status.v3'])
def test_observe_consumer_preserves_legacy_and_explicit_operational_semantics(journal, schema):
    value = journal.status()
    if schema != 'ahc.memory.native.status.v3':
        value.pop('integrity')
        value.pop('observation_scope')
    value['schema_version'] = schema
    observed = service(lambda **kw: {'status': 'not-found'}, status=lambda: value).status(session())
    assert observed == value
    value['status'] = 'unavailable'
    assert observed['status'] == 'ready'


@pytest.mark.parametrize('mutation', ['omit-integrity', 'claim-integrity', 'omit-scope',
                                      'claim-whole', 'extra', 'capability-int'])
def test_v3_consumer_rejects_missing_or_forged_integrity_claims(journal, mutation):
    value = journal.status()
    if mutation == 'omit-integrity':
        value.pop('integrity')
    elif mutation == 'claim-integrity':
        value['integrity'] = 'verified'
    elif mutation == 'omit-scope':
        value.pop('observation_scope')
    elif mutation == 'claim-whole':
        value['observation_scope'] = 'whole-database'
    elif mutation == 'extra':
        value['private'] = 'TEST-ONLY-PRIVATE-SENTINEL'
    else:
        value['capabilities']['store_committed'] = 1
    with pytest.raises(MemoryObserveUnavailable, match='^memory status unavailable$'):
        service(lambda **kw: {}, status=lambda: value).status(session())


@pytest.mark.parametrize('operation', ['status', 'get', 'store', 'reconcile', 'search',
                                       'integrity_check'])
def test_transport_only_maintenance_uses_900_seconds(tmp_path, monkeypatch, operation):
    binding = parse(document())
    calls = []

    def completed(command, **kwargs):
        calls.append(kwargs)
        request = json.loads(kwargs['input'])
        assert request['operation'] == operation
        return subprocess.CompletedProcess(command, 0, json.dumps({'test': True}).encode(), b'')

    monkeypatch.setattr(process_module.subprocess, 'run', completed)
    adapter = process_module.MemPalaceMemoryProcess(binding=binding, scratch_root=tmp_path)
    assert adapter._call(operation, {}) == {'test': True}
    assert len(calls) == 1
    assert calls[0]['timeout'] == (900 if operation == 'integrity_check' else binding.timeout)
    assert not list(tmp_path.glob('palace-memory-*'))


@pytest.mark.parametrize('operation', ['status', 'store', 'integrity_check'])
def test_timeout_is_sanitized_and_not_retried(tmp_path, monkeypatch, operation):
    binding = parse(document())
    calls = []

    def timeout(command, **kwargs):
        calls.append(kwargs)
        raise subprocess.TimeoutExpired(['TEST-ONLY-SECRET'], kwargs['timeout'],
                                        output=b'TEST-ONLY-SECRET', stderr=b'TEST-ONLY-SECRET')

    monkeypatch.setattr(process_module.subprocess, 'run', timeout)
    adapter = process_module.MemPalaceMemoryProcess(binding=binding, scratch_root=tmp_path)
    with pytest.raises(NativeMemoryUnavailable, match='^MemPalace operation timed out$'):
        adapter._call(operation, {})
    assert len(calls) == 1
    assert not list(tmp_path.glob('palace-memory-*'))


def test_real_worker_reopen_status_get_reconcile_integrity_without_dependencies(tmp_path):
    class WorkerEmbedder(Embedder):
        identity = MODEL_IDENTITY

    journal = MemoryJournal(tmp_path / 'worker-journal', embedder=WorkerEmbedder(),
                            projection=Projection(), create=True)
    original = journal.store(scope='private-hexi', operation_id='retained',
        observation={'kind': 'note', 'summary': 'fixture only', 'detail': {}}, index=False)
    with journal._connect() as db:
        db.execute('UPDATE records SET indexed=1')
    config = document()['bindings']['hexi.durable-memory']
    config.update(root=str(journal.root), provider_digest=mempalace_identity(),
                  credential_file=str(tmp_path / 'absent-credential'),
                  model_root=str(tmp_path / 'absent-model'), qdrant_url='http://127.0.0.1:1')
    binding = MemPalaceMemoryBinding(resource_id='hexi.durable-memory', **config)
    adapter = process_module.MemPalaceMemoryProcess(binding=binding, scratch_root=tmp_path)
    before = journal.database.read_bytes()
    assert service(adapter.get, adapter.status).status(session())['integrity'] == 'not-checked'
    assert adapter.get(scope=binding.scope, record_ref=original['record_ref']) == original
    assert adapter.reconcile(scope=binding.scope, operation_id='retained',
                             request_digest=original['request_digest']) == original
    assert adapter.integrity_check() == integrity_result('ok')
    assert journal.database.read_bytes() == before
    assert not Path(binding.credential_file).exists() and not Path(binding.model_root).exists()
    with pytest.raises(NativeMemoryNotDispatched):
        adapter._call('integrity_check', {'timeout': 9000})
    with pytest.raises(NativeMemoryNotDispatched):
        adapter.get(scope='foreign', record_ref=original['record_ref'])
    assert journal.database.read_bytes() == before
