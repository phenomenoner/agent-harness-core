import pytest
from test_child_dispatch import state

from ahc.application.child_dispatch import ChildDispatchService
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_backup import backup_runtime_database


@pytest.mark.parametrize('failure,expected', [
    ('cancel', 'cancellation-observed'), ('deadline', 'deadline-observed'),
    ('run', 'execution-unverified'), ('close', 'cleanup-unverified'),
])
def test_unknown_observations_survive_reopen_and_backup(tmp_path, failure, expected):
    store, request, clock, children, envelope = state(tmp_path)
    cancelled = [False]

    class Port:
        def run(self, *args, **kwargs):
            if failure == 'cancel':
                cancelled[0] = True
            if failure == 'deadline':
                clock[0] = 201
            if failure == 'run':
                raise OSError('private provider message')
            return 'answer'

        def close(self):
            if failure == 'close':
                raise OSError('private cleanup message')

    result = ChildDispatchService(children, lambda _: Port()).dispatch(
        envelope, 'token', cancelled=lambda: cancelled[0])
    assert result['state'] == 'unknown' and expected in result['observations']
    reopened = ChildDispatchRepository(RuntimeRepository(store.root), request)
    assert reopened.read(envelope.parent)[0] == result
    assert 'private' not in str(result)
    backup = tmp_path / 'backup'
    backup_runtime_database(store, backup)
    import sqlite3
    with sqlite3.connect(backup / 'runtime.sqlite') as connection:
        assert expected in [row[0] for row in connection.execute(
            'SELECT code FROM runtime_child_observations WHERE child_id=?', (envelope.child_id,))]


def test_old_rows_have_no_invented_observation_and_reads_are_inert(tmp_path):
    store, _, _, children, envelope = state(tmp_path)
    children.admit(envelope)
    with store._read() as connection:
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_child_observations'").fetchone()
    assert 'observations' not in children.read(envelope.parent)[0]
    with store._read() as connection:
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_child_observations'").fetchone()


def test_recovery_records_unfinished_claim_without_claiming_worker_cause(tmp_path):
    _, _, _, children, envelope = state(tmp_path)
    children.admit(envelope)
    children.claim(envelope.parent, envelope.child_id, 'token')
    result = children.reconcile(envelope.parent)[0]
    assert result['state'] == 'unknown'
    assert result['observations'] == ['unfinished-claim-recovered']


def test_foreign_observation_binding_rejects_on_read(tmp_path):
    from ahc.infrastructure.working_repository import WorkingSetCorrupt

    store, _, _, children, envelope = state(tmp_path)
    children.admit(envelope)
    children.cancel(envelope.parent, envelope.child_id)
    store._mutate(lambda connection: connection.execute(
        "UPDATE runtime_child_observations SET envelope_digest=?", ('f' * 64,)))
    with pytest.raises(WorkingSetCorrupt):
        children.read(envelope.parent)
