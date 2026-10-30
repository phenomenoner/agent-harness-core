import json
from types import SimpleNamespace

from ahc import AppConfig, Supervisor
from ahc.cli.foreground_control_pump import ChannelControlPump
from ahc.domain.sessions import ContractError
from ahc.infrastructure.persistence import StateStore


def test_unknown_turn_preserves_invoke_and_cleanup_code_without_replay(tmp_path):
    from test_runtime_core import setup

    from ahc.application.runtime_service import RuntimeService

    store, current, request = setup(tmp_path)

    class Backend:
        calls = 0

        def invoke(self, request):
            self.calls += 1
            try:
                raise ValueError("PRIVATE-INVOKE")
            finally:
                raise OSError("PRIVATE-CLEANUP")

    backend = Backend()
    turn = RuntimeService(store, backend).execute(current, request)
    trace = turn.result["failure_trace"]
    assert trace["exception_type"] == "OSError"
    assert trace["previous"]["exception_type"] == "ValueError"
    assert trace["frames"][-1]["module"] == "ahc.application.runtime_service"
    assert turn.result["unresolved_effects"] and not turn.result["accepted"]
    assert "PRIVATE" not in json.dumps(turn.to_dict())
    reopened = type(store)(store.root)
    assert reopened.read_turn(request.session, request.operation_id) == turn
    assert RuntimeService(reopened, backend).execute(
        reopened.read_current(current.session.ref.virtual_session_id), request) == turn
    assert backend.calls == 1


def test_control_pump_delivers_once_after_slow_shared_read(tmp_path):
    import threading
    import time

    from test_channel_controls import make

    store, _, _, route, controls, _, submit = make(tmp_path)
    event = submit("/help")
    sent, calls = threading.Event(), []

    def send(*args):
        calls.append(args)
        sent.set()
        return "receipt"

    pump = ChannelControlPump(SimpleNamespace(
        document={"channels": []}, deliver_one=controls.deliver_one), None, None)
    pump.transports = {route: SimpleNamespace(send=send)}
    with store._read():
        pump.start()
        time.sleep(1.2)
    try:
        assert sent.wait(3)
    finally:
        pump.close()
    assert not pump.failed and len(calls) == 1
    with store._read() as c:
        assert tuple(c.execute("SELECT state,receipt FROM channel_controls WHERE operation_id=?",
                               (event.operation_id,)).fetchone()) == ("sent", "receipt")


def test_receiver_failure_keeps_code_location_without_private_content(tmp_path):
    def fail(_transports):
        raise RuntimeError("PRIVATE-CREDENTIAL C:/private/auth.json user-message")

    controls = SimpleNamespace(document={"channels": []}, deliver_one=fail)
    pump = ChannelControlPump(controls, None, None)

    class Runtime:
        channel_receiver = pump

        def start(self):
            pump._run()

        def step(self, **kwargs):
            if pump.failed:
                raise ContractError("channel control receiver failed")

        def close(self):
            pump.close()

    config = AppConfig(state_dir=tmp_path / "supervisor")
    assert Supervisor(config, runtime=Runtime()).run(max_runtime_seconds=1) == 4
    result = StateStore(config.state_dir).read()
    diagnostic = json.loads((config.state_dir / "runtime-failure.json").read_text())
    assert diagnostic["supervisor"] == result["supervisor"]
    detail = diagnostic["runtime_failures"][0]
    assert detail["phase"] == "step" and detail["exception_type"] == "ContractError"
    assert detail["channel_receiver"]["exception_type"] == "RuntimeError"
    assert detail["channel_receiver"]["frames"][-1]["module"] == "ahc.cli.foreground_control_pump"
    assert detail["channel_receiver"]["frames"][-1]["function"] == "_run"
    encoded = json.dumps([result, diagnostic])
    private = ("PRIVATE-CREDENTIAL", "C:/private", "user-message")
    assert all(value not in encoded for value in private)


def test_runtime_failure_phase_and_cleanup_are_separate(tmp_path):
    class Runtime:
        def start(self):
            raise ValueError("PRIVATE-START")

        def close(self):
            raise OSError("PRIVATE-CLOSE")

    config = AppConfig(state_dir=tmp_path / "supervisor")
    assert Supervisor(config, runtime=Runtime()).run(max_runtime_seconds=0) == 4
    result = StateStore(config.state_dir).read()
    diagnostic = json.loads((config.state_dir / "runtime-failure.json").read_text())
    assert [(d["phase"], d["exception_type"]) for d in diagnostic["runtime_failures"]] == [
        ("start", "ValueError"), ("close", "OSError")]
    assert "PRIVATE" not in json.dumps([result, diagnostic])
