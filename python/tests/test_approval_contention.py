import sqlite3
import threading
from types import SimpleNamespace

import pytest
from test_runtime_approval_store import setup

from ahc.application.pending_approval import PendingApprovalPort
from ahc.domain.runtime_approval import ApprovalDecision
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_manual_decision_commit_waits_for_short_external_read_without_replay(tmp_path):
    repo, _, admitted, request, _ = setup(tmp_path)
    threaded = ThreadedRuntimeRepository(repo.root)
    store = RuntimeApprovalStore(threaded, clock=lambda: 91)
    store.publish(admitted, request)
    store.respond(admitted.session, request.digest, "approve-once")
    locked, commit_started = threading.Event(), threading.Event()
    reader_errors = []

    def reader():
        try:
            with sqlite3.connect(repo.db_path) as connection:
                connection.execute("BEGIN")
                connection.execute("SELECT * FROM runtime_approvals").fetchall()
                locked.set()
                assert commit_started.wait(3)
                # Keep the read lock across the first COMMIT attempt.
                threading.Event().wait(0.08)
                connection.rollback()
        except BaseException as error:
            reader_errors.append(error)

    worker = threading.Thread(target=reader)
    worker.start()
    assert locked.wait(3)
    callbacks = []
    original = threaded._mutate

    def observed(operation, **kwargs):
        def run(connection):
            connection.set_trace_callback(
                lambda sql: commit_started.set() if sql == "COMMIT" else None)
            callbacks.append(True)
            return operation(connection)
        return original(run, **kwargs)

    threaded._mutate = observed
    try:
        decision = store.consume(admitted.session, request.digest)
    finally:
        commit_started.set()
        worker.join(3)
    assert not worker.is_alive() and not reader_errors
    assert decision.outcome == "approve-once" and callbacks == [True]
    assert store.query(admitted.session, request.digest)["state"] == "consumed"


@pytest.mark.parametrize("phase", ["cancel", "publish", "notify", "consume"])
def test_pending_busy_never_grants_or_repeats_an_approval_phase(tmp_path, phase):
    _, _, admitted, request, _ = setup(tmp_path)
    calls = []

    def call(name, result=None):
        calls.append(name)
        if name == phase:
            raise WorkingWriterBusy("private lock detail")
        return result

    store = SimpleNamespace(
        publish=lambda *_: call("publish"),
        consume=lambda *_ , **__: call("consume", ApprovalDecision(request.digest, "approve-once")))
    port = PendingApprovalPort(store, admitted, clock=lambda: 0, wall_clock=lambda: 90,
        notifier=lambda *_: call("notify"), wait=lambda *_: pytest.fail("busy phase replayed"))
    assert port.decide(request, cancelled=lambda: call("cancel", False)).outcome == "unavailable"
    assert calls.count(phase) == 1


def test_busy_commit_exhaustion_rolls_back_answer_consumption(tmp_path):
    repo, _, admitted, request, _ = setup(tmp_path)
    store = RuntimeApprovalStore(ThreadedRuntimeRepository(repo.root), clock=lambda: 91)
    store.publish(admitted, request)
    store.respond(admitted.session, request.digest, "approve-once")
    with sqlite3.connect(repo.db_path) as reader:
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM runtime_approvals").fetchall()
        with pytest.raises(WorkingWriterBusy):
            store.consume(admitted.session, request.digest)
        reader.rollback()
    assert store.query(admitted.session, request.digest)["state"] == "answered"


@pytest.mark.parametrize("busy_call", [1, 2, 3, 4])
def test_unavailable_cancellation_read_never_authorizes_tool(busy_call):
    from test_channel_tool_approval import admitted, authorize, Port
    from ahc.application.channel_tool_approval import ChannelToolGate

    calls = []
    def cancelled():
        calls.append(True)
        if len(calls) == busy_call:
            raise WorkingWriterBusy("private cancellation read")
        return False
    port = Port()
    gate = ChannelToolGate(admitted(), port, cancelled=cancelled,
                           clock=lambda: 10, wall_clock=lambda: 20)
    assert authorize(gate) is False
    assert len(calls) == busy_call and len(port.requests) <= 1
