"""Real child ledger overlap and exact-worker lifetime boundaries, without providers."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest
from test_runtime_core import setup

from ahc.application.child_dispatch import ChildDispatchService
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.child_worker_repository import ChildWorkerRepository
from ahc.infrastructure.runtime_execution_scope import permits_workspace_scope
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository


def prepared(tmp_path):
    base, snapshot, request = setup(tmp_path)
    store = ThreadedRuntimeRepository(base.root)
    store.begin(snapshot, request)
    children = ChildDispatchRepository(store, request, clock=lambda: 100, monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("first", children.parent, "work", "workspace", 200)
    return store, request, children, envelope


def test_workers_overlap_and_cannot_read_or_mutate_siblings(tmp_path):
    store, request, children, envelope = prepared(tmp_path)
    barrier = Barrier(2, timeout=5)
    with store.execution_lock(), store.workspace_turn_scope(request):
        workers = [ChildWorkerRepository(children, replace(envelope, child_id=name))
                   for name in ("first", "second")]

        def run(worker):
            own, parent = worker.envelope, worker.parent
            sibling = "second" if own.child_id == "first" else "first"

            class Port:
                def run(self, item, *, cancelled):
                    barrier.wait()
                    assert not permits_workspace_scope(store, request.session, "workspace")
                    assert [row["child_id"] for row in worker.read(parent)] == [own.child_id]
                    for action in (
                        lambda: worker.claim(parent, sibling, "token"),
                        lambda: worker.cancel(parent, sibling),
                        lambda: worker.complete(parent, sibling, "token", "answer"),
                        lambda: worker.mark_unknown(parent, sibling, "token"),
                        lambda: worker.should_stop(parent, sibling, "token"),
                        lambda: worker.admit(replace(own, child_id=sibling)),
                        lambda: worker.reconcile(parent),
                    ):
                        with pytest.raises(ContractError):
                            action()
                    with worker.backend_admission(item, "token"):
                        assert not cancelled()
                    return own.child_id

                def close(self):
                    pass

            with worker.permit.activate():
                return ChildDispatchService(worker, lambda _: Port()).dispatch(own, "token")

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(run, workers))
    assert [item["state"] for item in results] == ["terminal", "terminal"]
    assert len(children.read(children.parent)) == 2
    for worker in workers:
        with pytest.raises(ContractError):
            worker.read(worker.parent)
        with pytest.raises(ContractError), worker.permit.activate():
            pytest.fail("worker replayed")


def test_unused_worker_is_revoked_with_parent_and_foreign_envelope_rejected(tmp_path):
    store, request, children, envelope = prepared(tmp_path)
    with pytest.raises(ContractError):
        ChildWorkerRepository(children, envelope)
    with store.execution_lock(), store.workspace_turn_scope(request):
        worker = ChildWorkerRepository(children, envelope)
        with pytest.raises(ContractError):
            ChildWorkerRepository(children, replace(envelope, execution_kind="minion"))
        other = ChildWorkerRepository(children, replace(envelope, child_id="second"))
        with other.permit.activate():
            with ThreadPoolExecutor(max_workers=1) as executor, pytest.raises(ContractError):
                executor.submit(other.read, other.parent).result(timeout=5)
            with pytest.raises(ContractError):
                other.admit(envelope)
            with pytest.raises(ContractError), other.backend_admission(envelope, "token"):
                pytest.fail("foreign envelope admitted")
    with pytest.raises(ContractError), worker.permit.activate():
        pytest.fail("expired parent reused")


@pytest.mark.parametrize("failure", ["cancel", "cleanup"])
def test_worker_retains_unknown_and_pending_cleanup_on_failure(tmp_path, failure):
    store, request, children, envelope = prepared(tmp_path)
    stopped, closes = [], []
    with store.execution_lock(), store.workspace_turn_scope(request):
        worker = ChildWorkerRepository(children, envelope)

        class Port:
            def run(self, item, *, cancelled):
                if failure == "cancel":
                    stopped.append(True)
                    assert cancelled()
                return "answer"

            def close(self):
                closes.append(True)
                if failure == "cleanup":
                    raise RuntimeError("private close detail")

        service = ChildDispatchService(worker, lambda _: Port())

        def run():
            with worker.permit.activate():
                return service.dispatch(envelope, "token", cancelled=lambda: bool(stopped))

        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(run).result(timeout=5)
    assert result["state"] == "unknown" and result["result"] is None
    assert service.cleanup_pending == (failure == "cleanup")
    assert closes == [True]
    assert children.read(children.parent)[0]["state"] == "unknown"
