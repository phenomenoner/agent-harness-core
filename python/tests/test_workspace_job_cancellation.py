"""Real WSL-adapter cancellation must not wait for its active execution lock."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from test_runtime_core import setup
from test_wsl_python_workspace import Runner, Transport, open_workspace

from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.application.workspace_tools import WorkspaceTools


class SilentBlockingTransport(Transport):
    def __init__(self):
        super().__init__()
        self.requests = []
        self.execute_waiting = threading.Event()
        self.release_receive = threading.Event()
        self.close_entered = threading.Event()

    def send(self, request, *, deadline):
        self.requests.append(request)
        super().send(request, deadline=deadline)

    def receive(self, *, deadline):
        if self.request["operation"] == "execute":
            self.execute_waiting.set()
            self.release_receive.wait(10)
            if self.closed:
                raise OSError("transport closed")
        return super().receive(deadline=deadline)

    def close(self):
        self.close_entered.set()
        super().close()
        self.release_receive.set()


def _invoke(tools, action, *, job_id="cancel-me", **fields):
    return tools.invoke("workspace.job", {
        "expected": tools.workspace.identity.to_dict(),
        "action": action,
        "job_id": job_id,
        **fields,
    })


def test_real_wsl_job_cancel_is_prompt_and_waits_for_owned_cleanup(monkeypatch, tmp_path):
    store, snapshot, _ = setup(tmp_path)
    transport = SilentBlockingTransport()
    runner = Runner()
    runner.transport = transport
    created = []

    def factory(identity, checkpoint):
        assert checkpoint is None
        port, _, custody = open_workspace(
            monkeypatch, identity=identity, runner=runner,
        )
        created.append((port, custody))
        return port

    workspace = RuntimeWorkspace.open(store, snapshot, "work", factory)
    tools = WorkspaceTools(workspace)
    original_port, original_custody = created[0]
    cleanup_entered = threading.Event()
    allow_lease_close = threading.Event()
    original_lease_close = original_custody.lease.close

    def gated_lease_close():
        cleanup_entered.set()
        allow_lease_close.wait(5)
        original_lease_close()

    original_custody.lease.close = gated_lease_close
    replacement_port = None
    cancel_future = None
    cancel_result = None
    cancel_error = None
    timely_cleanup = False
    returned_before_cleanup = False
    cancel_started = None
    cancel_elapsed = None
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        with workspace.turn_scope(time.monotonic() + 30):
            try:
                started = _invoke(
                    tools, "start", code="answer = 42", expected_seconds=10,
                    wait_seconds=0,
                )
                assert started["result"]["status"] == "running"
                assert transport.execute_waiting.wait(1)
                assert original_port.identity.revision == 1

                # A different generation must not be closed through the stale job slot.
                replacement_identity = replace(
                    original_port.identity,
                    generation=original_port.identity.generation + 1,
                    revision=0,
                )
                replacement_runner = Runner()
                replacement_port, _, replacement_custody = open_workspace(
                    monkeypatch, identity=replacement_identity, runner=replacement_runner,
                )
                workspace.port = replacement_port
                mismatched = _invoke(tools, "cancel")
                assert mismatched["disposition"] == "rejected"
                assert not replacement_runner.transport.closed
                assert not replacement_custody.lease.closed
                assert not transport.closed
                workspace.port = original_port

                cancel_started = time.monotonic()
                cancel_future = pool.submit(_invoke, tools, "cancel")
                timely_cleanup = cleanup_entered.wait(0.75)
                returned_before_cleanup = cancel_future.done()
                if timely_cleanup:
                    # Transport closure has started, but custody close is gated.
                    assert not original_custody.lease.closed
                    assert transport.closed
                    assert not returned_before_cleanup
                else:
                    # Fail-first teardown for the old lock-held close path.
                    transport.release_receive.set()
                    assert cleanup_entered.wait(2)
                allow_lease_close.set()
                cancel_result = cancel_future.result(timeout=2)
                cancel_elapsed = time.monotonic() - cancel_started
            finally:
                # Release gates before turn_scope's finish_turn can try cancellation.
                transport.release_receive.set()
                allow_lease_close.set()
                if cancel_future is not None and not cancel_future.done():
                    try:
                        cancel_result = cancel_future.result(timeout=3)
                    except BaseException as error:
                        cancel_error = error
    finally:
        transport.release_receive.set()
        allow_lease_close.set()
        if cancel_future is not None and not cancel_future.done():
            try:
                cancel_result = cancel_future.result(timeout=3)
            except BaseException as error:
                if cancel_error is None:
                    cancel_error = error
        pool.shutdown(wait=True)
        workspace.port = original_port
        if replacement_port is not None:
            replacement_port.close()
        workspace.close()

    assert timely_cleanup, "cancel did not reach owned cleanup while execute held the WSL lock"
    assert not returned_before_cleanup, "cancel reported stop before custody cleanup completed"
    assert cancel_error is None
    assert cancel_elapsed is not None and cancel_elapsed <= 2
    assert transport.close_entered.is_set()
    assert transport.closed and original_custody.lease.closed
    assert cancel_result["result"]["status"] == "cancelled"
    assert cancel_result["result"]["cancellation"]["execution_stopped"] is True
    assert len([request for request in transport.requests
                if request["operation"] == "execute"]) == 1
    status = _invoke(tools, "status")
    assert status["result"]["status"] == "cancelled"
