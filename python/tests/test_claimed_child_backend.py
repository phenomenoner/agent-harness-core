from dataclasses import replace

import pytest
from test_child_dispatch import state

from ahc.application.claimed_child_backend import ClaimedChildBackend
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("mode", ["claimed", "queued", "wrong-token", "expired", "changed"])
def test_claimed_child_admission_before_factory(tmp_path, mode):
    repo, parent, clock, children, envelope = state(tmp_path)
    children.admit(envelope)
    if mode != "queued":
        children.claim(envelope.parent, envelope.child_id, "token")
    if mode == "expired":
        clock[0] = envelope.deadline_unix_ms
    selected = replace(envelope, task_text="changed") if mode == "changed" else envelope
    request = replace(parent, operation_id=envelope.child_id, input_text=selected.task_text)
    calls, closes = [], []

    class Backend:
        def invoke(self, bound):
            assert bound == request
            calls.append("invoke")
            return "observation"

        def close(self):
            closes.append(True)

    def factory():
        calls.append("create")
        return Backend()

    backend = ClaimedChildBackend(children, selected,
        "other" if mode == "wrong-token" else "token", request, factory)
    if mode == "claimed":
        assert backend.invoke(request) == "observation"
        assert calls == ["create", "invoke"]
        assert repo.read_turn(parent.session, envelope.child_id) is None
    else:
        with pytest.raises(ContractError, match="durable claim"):
            backend.invoke(request)
        assert calls == []
    backend.close()
    assert len(closes) == (1 if mode == "claimed" else 0)
    with pytest.raises(ContractError, match="replayed"):
        backend.invoke(request)


def test_child_cancel_during_factory_closes_without_invoking(tmp_path):
    _, parent, _, children, envelope = state(tmp_path)
    children.admit(envelope)
    children.claim(envelope.parent, envelope.child_id, "token")
    request = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text)
    closed = []

    class Backend:
        def invoke(self, request):
            pytest.fail("cancelled child invoked backend")

        def close(self):
            closed.append(True)

    def factory():
        children.cancel(envelope.parent, envelope.child_id)
        return Backend()

    backend = ClaimedChildBackend(children, envelope, "token", request, factory)
    with pytest.raises(ContractError, match="stopped"):
        backend.invoke(request)
    backend.close()
    assert closed == [True]


def test_parent_turn_dispatch_uses_child_admission_and_mailbox(tmp_path):
    from ahc.adapters.child_agent_backend import ChildAgentBackend
    from ahc.application.child_dispatch import ChildDispatchService
    from ahc.domain.agent_backend import AgentTurnObservation

    repo, parent, clock, children, envelope = state(tmp_path)
    request = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text,
                      capabilities=(), deadline=0.05)
    calls = []

    class Backend:
        def invoke(self, bound):
            calls.append("invoke")
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1, output_text="answer")

        def close(self):
            calls.append("close")

    def admitted_factory(bound, *, cancelled):
        return ClaimedChildBackend(children, envelope, "token", bound, Backend)

    service = ChildDispatchService(children, lambda _: ChildAgentBackend(
        envelope, request, admitted_factory, parent_request=parent,
        monotonic=lambda: 0, wall_ms=lambda: clock[0]))
    with repo.execution_lock(), repo.workspace_turn_scope(parent):
        result = service.dispatch(envelope, "token")
        assert result["state"] == "terminal" and result["result"] == {"output_text": "answer"}
        assert service.dispatch(envelope, "other")["state"] == "terminal"
    assert calls == ["invoke", "close"]
    assert repo.read_turn(parent.session, envelope.child_id) is None
