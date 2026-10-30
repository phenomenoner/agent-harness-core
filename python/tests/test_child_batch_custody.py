from dataclasses import replace
from threading import Barrier

import pytest
from test_backend_custody import Runner
from test_runtime_core import setup

from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import LinuxUnitIdentity
from ahc.infrastructure.child_backend_custody import ChildBackendCustody
from ahc.infrastructure.child_batch_runner import ChildBatchRunner
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.runtime_execution_scope import permits_workspace_scope
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_real_custody_borrows_only_exact_worker_parent_lease(tmp_path):
    base, snapshot, request = setup(tmp_path)
    workspace = base.reserve_workspace_generation(snapshot, request.workspace_scope)
    store = ThreadedRuntimeRepository(base.root)
    store.begin(store.read_current('session'), request)
    children = ChildDispatchRepository(store, request, clock=lambda: 100,
                                       monotonic_clock=lambda: 0)
    envelopes = [ChildWorkEnvelope(name, children.parent, 'task', request.workspace_scope, 200)
                 for name in ('first', 'second')]
    barrier = Barrier(2, timeout=5)
    retained = []

    def factory(worker, envelope, token):
        custody = ChildBackendCustody(store, envelope, worker_permit=worker.permit)
        with pytest.raises(ContractError):
            ChildBackendCustody(store, replace(envelope, child_id='foreign'),
                                worker_permit=worker.permit)
        unit = LinuxUnitIdentity('AHC-Test', '12345678-1234-1234-1234-123456789abc',
            'ahc-backend-' + ('a' if envelope.child_id == 'first' else 'c') * 32 + '.service',
            'b' * 32)
        runner = Runner(unit)
        runner.create = lambda *args, **kwargs: unit

        class Port:
            lease = None

            def run(self, received, *, cancelled):
                assert not permits_workspace_scope(store, request.session, request.workspace_scope)
                with worker.backend_admission(envelope, token):
                    self.lease = custody.acquire(request.session, request.workspace_scope)
                    custody._worker_permit = None
                    try:
                        with pytest.raises(WorkingWriterBusy):
                            custody.recover(self.lease, runner)
                    finally:
                        custody._worker_permit = worker.permit
                    assert custody.recover(self.lease, runner) == ()
                    barrier.wait()
                    custody.create(self.lease, workspace, runner, ['/usr/bin/cat'], lifetime_seconds=1)
                    with custody.attachment(self.lease, unit):
                        assert custody._held(self.lease)[0][2] == unit
                    retained.append((custody, worker.permit, envelope))
                    return envelope.child_id

            def close(self):
                if self.lease:
                    custody.recover(self.lease, runner)
                    self.lease.close()
                    self.lease = None
        return Port()

    batch = ChildBatchRunner(children)
    with store.execution_lock(), store.workspace_turn_scope(request):
        results = batch.dispatch(envelopes, factory, concurrency=2)
        assert [row['state'] for row in results] == ['terminal', 'terminal']
        assert [row['result']['output_text'] for row in results] == ['first', 'second']
        for custody, permit, envelope in retained:
            with pytest.raises(ContractError):
                permit.require(store, envelope)
            lease = custody.acquire(request.session, request.workspace_scope)
            try:
                assert custody.recover(lease, Runner(unit=None)) == ()
            finally:
                lease.close()
    batch.close()
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM backend_custody WHERE state='held'").fetchone()[0] == 0
