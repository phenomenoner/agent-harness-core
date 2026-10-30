"""Batch limits, sibling results, queue cancellation and parent cleanup handoff."""

from dataclasses import replace
from threading import Barrier, Event, get_ident

import pytest
from test_child_worker_repository import prepared

from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_batch_runner import ChildBatchRunner


def test_overlap_order_sibling_failure_and_parent_cleanup(tmp_path):
    store, request, children, envelope = prepared(tmp_path)
    batch = ChildBatchRunner(children)
    barrier, cleanup_fails, closed = Barrier(3, timeout=5), Event(), []
    cleanup_fails.set()
    parent_thread = get_ident()

    def factory(worker, item, token):
        class Port:
            def run(self, received, *, cancelled):
                with pytest.raises(ContractError):
                    batch.close()
                with worker.backend_admission(received, token):
                    barrier.wait()
                    if item.child_id == "failure":
                        raise RuntimeError("private failure")
                    return item.child_id

            def close(self):
                closed.append((item.child_id, get_ident()))
                if item.child_id == "cleanup" and cleanup_fails.is_set():
                    raise RuntimeError("private cleanup failure")

        return Port()

    items = [replace(envelope, child_id=name) for name in ("success", "failure", "cleanup")]
    with store.execution_lock(), store.workspace_turn_scope(request):
        results = batch.dispatch(items, factory)
        assert [row["state"] for row in results] == ["terminal", "unknown", "unknown"]
        assert batch.cleanup_pending
        with pytest.raises(ContractError):
            batch.dispatch(items, factory)
    # Cleanup happens after workers and the old turn grant have ended, under
    # the exact parent's execution lock; only unresolved cleanup is retried.
    with pytest.raises(ContractError):
        batch.close()
    cleanup_fails.clear()
    batch.close()
    batch.close()
    assert not batch.cleanup_pending
    assert [name for name, _ in closed].count("cleanup") == 3
    assert [name for name, _ in closed].count("success") == 1
    assert [name for name, _ in closed].count("failure") == 1
    assert closed[-1] == ("cleanup", parent_thread)
    with store.execution_lock(), store.workspace_turn_scope(request):
        replay = batch.dispatch(items, lambda *_: pytest.fail("replayed terminal/unknown"))
    assert [row["state"] for row in replay] == ["terminal", "unknown", "unknown"]
    batch.close()


def test_cancellation_stops_queued_factories(tmp_path):
    store, request, children, envelope = prepared(tmp_path)
    batch, stopped, calls = ChildBatchRunner(children), Event(), []

    def factory(worker, item, token):
        calls.append(item.child_id)

        class Port:
            def run(self, item, *, cancelled):
                stopped.set()
                assert cancelled()
                return "cancelled"

            def close(self):
                pass

        return Port()

    with store.execution_lock(), store.workspace_turn_scope(request):
        results = batch.dispatch([replace(envelope, child_id=str(i)) for i in range(4)],
                                 factory, cancelled=stopped.is_set, concurrency=1)
    assert calls == ["0"]
    assert [row["state"] for row in results] == ["unknown", "cancelled", "cancelled", "cancelled"]
    batch.close()


@pytest.mark.parametrize("invalid", ["duplicate", "scope", "empty", "concurrency"])
def test_full_validation_precedes_admission_or_factory(tmp_path, invalid):
    store, request, children, envelope = prepared(tmp_path)
    batch = ChildBatchRunner(children)
    items, concurrency = [envelope], 3
    if invalid == "duplicate":
        items *= 2
    elif invalid == "scope":
        items.append(replace(envelope, child_id="second", artifact_scope="foreign"))
    elif invalid == "empty":
        items = []
    else:
        concurrency = 0
    with store.execution_lock(), store.workspace_turn_scope(request), pytest.raises(ContractError):
        batch.dispatch(items, lambda *_: pytest.fail("invalid batch started"),
                       concurrency=concurrency)
    assert children.read(children.parent) == ()


def test_existing_children_do_not_create_an_extra_sixteen_call_ceiling(tmp_path):
    store, request, children, envelope = prepared(tmp_path)
    batch = ChildBatchRunner(children)
    with store.execution_lock(), store.workspace_turn_scope(request):
        for i in range(16):
            children.admit(replace(envelope, child_id=str(i)))
        batch._validate([envelope], lambda *_: None, lambda: False, 3)
    assert len(children.read(children.parent)) == 16


def test_queued_deadline_expiry_does_not_start_another_port(tmp_path):
    store, request, children, envelope = prepared(tmp_path)
    wall, calls = [100], []
    children.clock = lambda: wall[0]
    batch = ChildBatchRunner(children)

    def factory(worker, item, token):
        calls.append(item.child_id)

        class Port:
            def run(self, item, *, cancelled):
                wall[0] = 201
                assert cancelled()
                return "late"

            def close(self):
                pass

        return Port()

    with store.execution_lock(), store.workspace_turn_scope(request):
        results = batch.dispatch([envelope, replace(envelope, child_id="second")],
                                 factory, concurrency=1)
    assert calls == ["first"]
    assert [row["state"] for row in results] == ["unknown", "cancelled"]
    batch.close()
