"""Positive pre-create refusal releases only its intent; ambiguous replies retain custody."""

import io
import json
from types import SimpleNamespace

import pytest
from test_backend_custody import ready_to_create
from test_foreground_runtime import setup

from ahc.adapters import linux_unit_helper, linux_unit_stream
from ahc.adapters.wsl_python_workspace import WslPythonWorkspace
from ahc.adapters.wsl_stream_runner import WslStreamRunner
from ahc.adapters.wsl_unit_runner import WslCreationIndeterminate
from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.domain.backend_creation import BackendCreationRejected
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.infrastructure.runtime_inbox import RuntimeInbox


class RefusalTransport:
    closed = False

    def __init__(self, change=None):
        self.change = change or {}

    def send(self, request, **kwargs):
        self.request = request

    def receive(self, **kwargs):
        return {"ok": False, "error": "interpreter-absent", "created": False,
                "unit": self.request["unit"], **self.change}

    def close(self):
        self.closed = True


def test_controller_refuses_missing_interpreter_before_control(tmp_path, monkeypatch):
    request = {"action": "create", "unit": "ahc-backend-" + "a" * 32 + ".service",
               "command": [str(tmp_path / "missing-python")], "lifetime_seconds": 30}

    def forbidden(*args, **kwargs):
        pytest.fail("missing interpreter reached unit creation")

    helper = SimpleNamespace(control=forbidden, _object=linux_unit_helper._object,
                             ControlError=linux_unit_helper.ControlError)
    output = io.BytesIO()
    monkeypatch.setattr(linux_unit_stream.sys, "stdin",
                        io.TextIOWrapper(io.BytesIO((json.dumps(request) + "\n").encode())))
    monkeypatch.setattr(linux_unit_stream.sys, "stdout", io.TextIOWrapper(output))
    linux_unit_stream.serve(helper, persistent=True)
    assert json.loads(output.getvalue()) == {
        "ok": False, "error": "interpreter-absent", "created": False, "unit": request["unit"]}


@pytest.mark.parametrize("change", [None, {"unit": "foreign"}, {"created": 0},
                                    {"ok": 0}, {"extra": True}, {"error": "unverified"}])
def test_only_exact_refusal_resolves_pending_creation(tmp_path, change):
    repo, custody, lease, workspace, unit = ready_to_create(tmp_path)
    runner = WslStreamRunner("C:/Windows/System32/wsl.exe", unit.distribution, persistent=True)
    transport = RefusalTransport(change)
    runner._open_transport = lambda source: transport
    try:
        with pytest.raises(BackendCreationRejected if change is None else WslCreationIndeterminate):
            custody.create(lease, workspace, runner, ("/missing/python",), lifetime_seconds=30)
        with repo._read() as connection:
            assert connection.execute(
                "SELECT count(*) FROM backend_creation_intents WHERE state='pending'"
            ).fetchone()[0] == (0 if change is None else 1)
    finally:
        runner.close()
        lease.close()
    assert transport.closed


def test_remote_refusal_through_real_workspace_preserves_foreground_queue(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)
    runner = WslStreamRunner("C:/Windows/System32/wsl.exe", "AHC-Codex-Ubuntu-24.04",
                             persistent=True)
    transport = RefusalTransport()
    attempts = []

    def transport_factory(source):
        attempts.append(True)
        return transport

    runner._open_transport = transport_factory

    def open_workspace(snapshot):
        def factory(identity, checkpoint):
            return WslPythonWorkspace(runtime.store, runner, identity,
                effective_policy=EffectivePolicy(1, "manual", "read-only", True, "a" * 64, 30),
                environment_digest="a" * 64, cwd="/home/ahc/work",
                interpreter="/missing/python", lifetime_seconds=30)
        return RuntimeWorkspace.open(runtime.store, snapshot, "work", factory)

    runtime.open_workspace = open_workspace
    requests.append(request)
    inbox = RuntimeInbox(runtime.store)
    inbox.submit(request.session, request.operation_id, request.goal_digest, request.input_text)
    runtime.start()
    try:
        assert runtime.workspace_status()["reason_code"] == "remote-interpreter-absent"
        for _ in range(3):
            assert runtime.step() is None
        assert attempts == [True] and requests == [request] and invoked == []
        assert inbox.query(request.session, request.operation_id)["status"] == "pending"
        with runtime.store._read() as connection:
            count = connection.execute(
                "SELECT count(*) FROM backend_creation_intents").fetchone()[0]
            assert count == 0
        assert transport.closed
    finally:
        runtime.close()
    with runtime.store.foreground_lease(request.session):
        pass


def test_present_interpreter_later_failure_is_not_positive_refusal(tmp_path, monkeypatch):
    interpreter = tmp_path / "ordinary-file"
    interpreter.write_text("not executable")
    calls = []

    def control(request, **kwargs):
        calls.append(request)
        raise linux_unit_helper.ControlError("later creation failure")

    helper = SimpleNamespace(control=control, _object=linux_unit_helper._object,
                             ControlError=linux_unit_helper.ControlError)
    request = {"action": "create", "command": [str(interpreter)], "unit": "fixture"}
    output = io.BytesIO()
    monkeypatch.setattr(linux_unit_stream.sys, "stdin",
                        io.TextIOWrapper(io.BytesIO((json.dumps(request) + "\n").encode())))
    monkeypatch.setattr(linux_unit_stream.sys, "stdout", io.TextIOWrapper(output))
    linux_unit_stream.serve(helper, persistent=True)
    assert calls == [request]
    assert json.loads(output.getvalue()) == {"ok": False, "error": "unit stream unverified"}


def test_refusal_resolution_rejects_changed_intent(tmp_path):
    repo, custody, lease, workspace, _ = ready_to_create(tmp_path)

    class Changed:
        def create(self, *args, **kwargs):
            repo._mutate(lambda c: c.execute(
                "UPDATE backend_creation_intents SET workspace_json='foreign'"))
            raise BackendCreationRejected("refused")

    try:
        from ahc.domain.sessions import ContractError

        with pytest.raises(ContractError, match="identity changed"):
            custody.create(lease, workspace, Changed(), ("/missing/python",), lifetime_seconds=30)
        with repo._read() as connection:
            assert tuple(connection.execute(
                "SELECT workspace_json,state FROM backend_creation_intents").fetchone()) == (
                    "foreign", "pending")
    finally:
        lease.close()
