from types import SimpleNamespace

from test_channel_controls import make

from ahc.cli.foreground_control_pump import ChannelControlPump
from ahc.infrastructure.working_repository import WorkingWriterBusy


def pump(deliver_one):
    controls = SimpleNamespace(document={"channels": []}, deliver_one=deliver_one)
    return ChannelControlPump(controls, None, None)


def test_busy_before_claim_retries_once_then_delivers():
    calls = []

    def deliver_one(_transports):
        calls.append("deliver")
        if len(calls) == 1:
            raise WorkingWriterBusy("busy before claim")
        active.stop.set()

    active = pump(deliver_one)
    active._run()

    assert calls == ["deliver", "deliver"]
    assert not active.failed and not active.paused


def test_busy_after_send_preserves_sending_row_without_resend(tmp_path, monkeypatch):
    store, _, _, route, controls, _, submit = make(tmp_path)
    event = submit("/help")
    sent, deliveries = [], []

    class Transport:
        def send(self, *_args):
            sent.append("send")
            return "receipt"

    mutate = store._mutate
    mutation_count = 0

    def busy_after_claim(change):
        nonlocal mutation_count
        mutation_count += 1
        if mutation_count == 2:
            raise WorkingWriterBusy("busy after send")
        return mutate(change)

    monkeypatch.setattr(store, "_mutate", busy_after_claim)

    def deliver_one(transports):
        deliveries.append("deliver")
        try:
            return controls.deliver_one(transports)
        finally:
            if len(deliveries) == 2:
                active.stop.set()

    active = pump(deliver_one)
    active.transports = {route: Transport()}
    active._run()

    assert sent == ["send"] and deliveries == ["deliver", "deliver"]
    assert not active.failed and not active.paused
    with store._read() as connection:
        assert connection.execute(
            "SELECT state FROM channel_controls WHERE operation_id=?", (event.operation_id,)
        ).fetchone()[0] == "sending"


def test_repeated_busy_defers_at_normal_cadence_and_recovers_ingress():
    calls = []

    def deliver_one(_transports):
        calls.append("deliver")
        if len(calls) <= 4:
            raise WorkingWriterBusy("busy")

    active = pump(deliver_one)
    received = []
    def receive(**kwargs):
        received.append(True)
        active.stop.set()
    active.telegram = receive
    waits = []
    active.stop.wait = lambda delay: waits.append(delay)
    active._run()

    assert calls == ["deliver"] * 5 and received == [True]
    assert waits == [0.1] * 5
    assert not active.paused and not active.failed


def test_persistent_busy_remains_stoppable():
    calls = []
    def deliver_one(_transports):
        calls.append(True)
        raise WorkingWriterBusy("busy")
    active = pump(deliver_one)
    def wait(delay):
        assert delay == 0.1
        if len(calls) == 4:
            active.stop.set()
    active.stop.wait = wait
    active._run()
    assert len(calls) == 4 and not active.paused and not active.failed


def test_success_resets_consecutive_contention_budget():
    calls = []

    def deliver_one(_transports):
        calls.append("deliver")
        if len(calls) in (1, 3):
            raise WorkingWriterBusy("busy")
        if len(calls) == 4:
            active.stop.set()

    active = pump(deliver_one)
    active._run()
    assert len(calls) == 4 and not active.paused and not active.failed


def test_non_lock_exception_remains_fatal():
    def deliver_one(_transports):
        raise RuntimeError("fatal")

    active = pump(deliver_one)
    active._run()

    assert active.failed and not active.paused and active.stop.is_set()
    assert active.failure_trace["exception_type"] == "RuntimeError"
