import pytest

from test_channel_controls import make
from test_codex_client import setup
from test_codex_native_collaboration import spawn_started, spawn_completed, child_turn, child_status

from ahc.adapters.docker_workspace import WorkspaceUnavailable
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("error", [WorkspaceUnavailable("private startup"),
                                  BackendCustodyUnavailable("private pending creation")])
def test_unresolved_workspace_start_keeps_controls_and_input_without_replaying(tmp_path, error):
    store, current, _, route, controls, _, submit = make(tmp_path)
    pending = submit("work", "unresolved-start")
    calls, sent = [], []
    class Receiver:
        workspace_independent = True
        failed = False
        def start(self): calls.append("receiver-start")
        def close(self): calls.append("receiver-close")
    def unavailable(snapshot):
        calls.append("workspace-start")
        raise error
    def forbidden(*args, **kwargs):
        pytest.fail("unresolved startup must not admit or retry work")
    runtime = ForegroundRuntime(store, current.session, forbidden, forbidden,
        open_workspace=unavailable, channel_receiver=Receiver())
    controls.bind_admission_status(runtime.admission_status)
    class Transport:
        def send(self, target, payload, identifier):
            assert target == route
            sent.append(payload["output_text"])
            return "warning-receipt"
    runtime.start()
    try:
        assert runtime.state == "running" and not runtime.admission_status()["ready"]
        for _ in range(3):
            assert runtime.step() is None
            controls.deliver_one({route: Transport()})
        assert calls == ["workspace-start", "receiver-start"]
        assert len(sent) == 1 and "訊息已保留" in sent[0]
        assert RuntimeInbox(store).query(current.session, pending.operation_id)["status"] == "pending"
        trace = runtime.workspace_status()["failure_trace"]
        assert trace["exception_type"] == type(error).__name__ and "private" not in str(trace)
    finally:
        runtime.close()


@pytest.mark.parametrize("status", [{"type": "active", "activeFlags": ["newFlag"]},
                                     {"type": "newState"}, None, []])
def test_advisory_status_drift_keeps_warning_and_requires_real_parent_final(status):
    peer, client, request = setup()
    peer.messages.insert(3, {"method": "thread/status/changed",
        "params": {"threadId": "thread", "status": status}})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.backend_diagnostic["code"] == "advisory-status-ignored"
    assert result.backend_diagnostic["last_method"] == "thread/status/changed"


def test_child_advisory_drift_preserves_parent_warning_and_child_terminal_requirement():
    peer, client, request = setup()
    notice = child_status()
    notice["params"]["status"] = {"type": "active", "activeFlags": ["newFlag"]}
    peer.messages[3:3] = [spawn_started(), spawn_completed(), child_turn(), notice,
                         child_turn("turn/completed", status="completed")]
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.backend_diagnostic["code"] == "advisory-status-ignored"


def test_real_later_protocol_failure_takes_precedence_over_advisory_warning():
    peer, client, request = setup()
    peer.messages[3:3] = [
        {"method": "thread/status/changed", "params": {"threadId": "thread", "status": None}},
        {"method": "turn/completed", "params": {"threadId": "foreign", "turn": {"id": "turn", "status": "completed"}}},
    ]
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.output_text == ""
    assert result.backend_diagnostic["code"] != "advisory-status-ignored"


def test_foreign_liveness_does_not_enroll_a_child_or_change_parent_identity():
    peer, client, request = setup()
    peer.messages.insert(1, {"method": "thread/status/changed",
        "params": {"threadId": "foreign", "status": {"type": "idle"}}})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.backend_thread == "thread"
    assert client._native_collaboration.children == {}
    assert result.backend_diagnostic["code"] == "advisory-status-ignored"
