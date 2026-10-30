from dataclasses import replace

import pytest
from test_child_dispatch import state

from ahc.adapters.child_agent_backend import ChildAgentBackend
from ahc.application.child_dispatch import ChildDispatchService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("outcome", ["success", "foreign", "unknown", "cancelled", "close-failed"])
def test_child_backend_mailbox_no_replay_and_cleanup(tmp_path, outcome):
    _, parent, clock, children, envelope = state(tmp_path)
    request = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text,
                      deadline=0.05, capabilities=(), backend_thread=None)
    calls, closes = [], []
    cancelled = [False]

    class Backend:
        def invoke(self, admitted):
            calls.append(admitted)
            if outcome == "cancelled":
                cancelled[0] = True
            return AgentTurnObservation(
                parent if outcome == "foreign" else admitted,
                "unknown" if outcome == "unknown" else "completed", "fixture", 1, 1,
                output_text="child answer")

        def close(self):
            closes.append(True)
            if outcome == "close-failed" and len(closes) == 1:
                raise RuntimeError("fixture close failure")

    def factory(bound, *, cancelled):
        assert bound == request and not cancelled()
        return Backend()

    service = ChildDispatchService(children, lambda _: ChildAgentBackend(
        envelope, request, factory, parent_request=parent,
        monotonic=lambda: 0, wall_ms=lambda: clock[0]))
    result = service.dispatch(envelope, "token", cancelled=lambda: cancelled[0])
    assert result["state"] == ("terminal" if outcome == "success" else "unknown")
    if outcome == "success":
        assert result["result"] == {"output_text": "child answer"}
    service.close()
    again = service.dispatch(envelope, "other")
    assert again["state"] == result["state"] and calls == [request]
    assert len(closes) == (2 if outcome == "close-failed" else 1)


def test_child_backend_rejects_scope_and_deadline_before_factory(tmp_path):
    _, parent, clock, _, envelope = state(tmp_path)
    request = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text,
                      deadline=0.05, capabilities=(), backend_thread=None)
    calls = []
    for changed in (replace(request, workspace_scope="other"),
                    replace(request, capabilities=("workspace.execute",)),
                    replace(request, configuration_digest="f" * 64), parent):
        with pytest.raises(ContractError, match="binding"):
            ChildAgentBackend(envelope, changed, lambda *a, **k: calls.append(True),
                              parent_request=parent)
    port = ChildAgentBackend(envelope, replace(request, deadline=1),
        lambda *a, **k: calls.append(True), parent_request=parent,
        monotonic=lambda: 0, wall_ms=lambda: clock[0])
    with pytest.raises(ContractError, match="deadline"):
        port.run(envelope, cancelled=lambda: False)
    assert calls == []


def test_child_cannot_drop_parent_required_request_limit(tmp_path):
    from ahc.domain.runtime_turn import request_fingerprint

    _, parent, _, _, envelope = state(tmp_path)
    parent = replace(parent, upstream_request_limit=1)
    envelope = replace(envelope,
        parent=replace(envelope.parent, request_digest=request_fingerprint(parent)))
    request = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text,
                      deadline=0.05, capabilities=(), backend_thread=None,
                      upstream_request_limit=None)
    with pytest.raises(ContractError, match="binding"):
        ChildAgentBackend(envelope, request, lambda *a, **k: None, parent_request=parent)


@pytest.mark.parametrize("limit", [0, 1, 10])
def test_required_hard_request_limit_rejects_child_before_factory(tmp_path, limit):
    _, parent, _, _, envelope = state(tmp_path)
    request = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text,
                      deadline=0.05, capabilities=(), backend_thread=None,
                      upstream_request_limit=limit)
    calls = []
    with pytest.raises(ContractError, match="binding"):
        ChildAgentBackend(envelope, request, lambda *a, **k: calls.append(True),
                          parent_request=parent)
    assert calls == []
