import threading
import time

import pytest
from test_runtime_core import setup

from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_shared_writer_survives_slow_local_transaction(tmp_path):
    original, _, _ = setup(tmp_path)
    store = ThreadedRuntimeRepository(original.root)
    entered, outcomes = threading.Event(), []

    def writer():
        entered.set()
        try:
            outcomes.append(store._mutate(lambda c: c.execute("SELECT 42").fetchone()[0]))
        except WorkingWriterBusy:
            outcomes.append("busy")

    with store._read():
        worker = threading.Thread(target=writer)
        worker.start()
        assert entered.wait(1)
        time.sleep(1.2)
    worker.join(3)
    assert not worker.is_alive() and outcomes == [42]


@pytest.mark.parametrize("scope", ["writer_lock", "_read"])
def test_shared_wait_is_finite_and_does_not_enter_database_when_exhausted(tmp_path, scope):
    original, _, _ = setup(tmp_path)
    store = ThreadedRuntimeRepository(original.root)
    waits = []

    class HeldLock:
        def acquire(self, *, timeout):
            waits.append(timeout)
            return False

        def release(self):
            pytest.fail("unowned lock released")

    store._thread_writer = HeldLock()
    with pytest.raises(WorkingWriterBusy), getattr(store, scope)():
        pytest.fail("busy operation entered")
    assert waits == [10]


@pytest.mark.parametrize("scope", ["writer_lock", "_read"])
def test_threaded_writers_wait_but_external_owner_still_rejects(tmp_path, scope):
    original, _, _ = setup(tmp_path)
    store = ThreadedRuntimeRepository(original.root)
    started, finished, outcomes = threading.Event(), threading.Event(), []

    def write():
        started.set()
        try:
            outcomes.append(store._mutate(
                lambda connection: connection.execute("SELECT 42").fetchone()[0]))
        except Exception as error:
            outcomes.append(type(error).__name__)
        finally:
            finished.set()

    with getattr(store, scope)():
        worker = threading.Thread(target=write)
        worker.start()
        assert started.wait(1)
        assert not finished.wait(0.05)
    worker.join(timeout=2)
    assert not worker.is_alive() and outcomes == [42]
    # A distinct repository instance retains the OS lock exclusion, even in this process.
    with RuntimeRepository(original.root).writer_lock(), pytest.raises(WorkingWriterBusy):
        store._mutate(lambda connection: pytest.fail("external owner bypassed"))


def test_default_repository_still_fails_fast_on_concurrent_writer(tmp_path):
    store, _, _ = setup(tmp_path)
    errors = []

    def write():
        try:
            store._mutate(lambda connection: None)
        except WorkingWriterBusy:
            errors.append("busy")

    with store.writer_lock():
        worker = threading.Thread(target=write)
        worker.start()
        worker.join(timeout=1)
        assert not worker.is_alive()
    assert errors == ["busy"]
