import threading

import pytest
from test_tool_batch import call, fixture

from ahc.domain.sessions import ContractError


def test_parallel_calls_overlap_but_keep_input_order_and_worker_bound():
    session, batch, port = fixture()
    port.concurrent_reads = True
    barrier = threading.Barrier(2, timeout=3)
    lock = threading.Lock()
    active = peak = 0

    def invoke(*args):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        barrier.wait()
        with lock:
            active -= 1
        return args[1].call_id

    port.invoke = invoke
    calls = tuple(call(f"item{i}") for i in range(6))
    result = batch.run(session, calls, deadline=1, concurrency=2)
    assert [r["state"] for r in result.receipts] == ["success"] * 6
    assert result.values == tuple(c.call_id for c in calls)
    assert peak == 2
    assert active == 0


def test_unadmitted_parallelism_does_not_consume_call_ids():
    session, batch, port = fixture()
    with pytest.raises(ContractError):
        batch.run(session, (call("one"),), deadline=1, concurrency=2)
    assert batch.run(session, (call("one"),), deadline=1).receipts[0]["state"] == "success"


def test_parallel_cancellation_preserves_running_unknown_and_queued_rejection():
    session, batch, port = fixture()
    port.concurrent_reads = True
    cancelled = threading.Event()
    barrier = threading.Barrier(2, timeout=3)

    def invoke(*args):
        barrier.wait()
        cancelled.set()
        return "late"

    port.invoke = invoke
    result = batch.run(session, tuple(call(f"item{i}") for i in range(4)),
                       deadline=1, concurrency=2, cancelled=cancelled.is_set)
    assert [r["state"] for r in result.receipts] == ["unknown", "unknown", "rejected", "rejected"]
    assert result.values == (None,) * 4
