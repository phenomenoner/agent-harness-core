"""Two live foreground owners share storage without crossing channel state."""

import pytest
from skill_test_support import goal
from test_workspace_tools import Port

from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_two_live_sessions_keep_workspace_backend_and_delivery_bound(tmp_path, platform):
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    runtimes, routes, events, calls, sent = [], [], [], [], []

    def build(index):
        session = VirtualSessionIdentity.initial(f"session-{index}", f"agent-{index}", "route")
        current = repo.create(WorkingSetSnapshot.initial(session, goal()))
        route = ChannelRoute(platform, "123", str(456 + index), str(789 + index), session)
        event = ChannelInboundEvent(route, "same-event-id", f"input-{index}")
        ChannelIngress(RuntimeInbox(repo), route).submit(event, current.goal.digest)
        workspace_id = f"work-{index}"

        def factory(request, tools, **kwargs):
            assert request.session == session and request.input_text == f"input-{index}"
            assert request.workspace_scope == workspace_id

            class Backend:
                def invoke(self, request):
                    calls.append((session.agent_id, request.operation_id))
                    other = runtimes[1 - index].workspace.identity.to_dict()
                    rejected = tools.invoke("workspace.execute", {"expected": other, "code": "x=0"})
                    assert rejected["disposition"] == "rejected"
                    result = tools.invoke("workspace.execute", {
                        "expected": tools.workspace.identity.to_dict(), "code": "x=42"})
                    assert result["disposition"] == "observed"
                    return AgentTurnObservation(request, "completed", "fixture", 1, 1,
                                                f"thread-{index}", "turn", f"answer-{index}")
            return Backend()

        runtime = ForegroundRuntime(
            repo, session,
            LocalRuntimeIngress(RuntimeInbox(repo), artifact_digest="a" * 64,
                                configuration_digest=lambda _: "b" * 64,
                                workspace_scope=workspace_id), factory,
            open_workspace=lambda snapshot: RuntimeService(repo, None).open_workspace(
                snapshot, workspace_id, Port),
        )
        return runtime, route, event

    for index in range(2):
        runtime, route, event = build(index)
        runtimes.append(runtime)
        routes.append(route)
        events.append(event)
    try:
        for runtime in runtimes:
            runtime.start()
        assert all(runtime.state == "running" for runtime in runtimes)
        for index in (1, 0):
            assert runtimes[index].step().result["accepted"]
        assert events[0].operation_id != events[1].operation_id
        assert all(runtime.workspace.port.calls == 1 for runtime in runtimes)

        class Transport:
            def send(self, route, payload, operation):
                sent.append((route, payload, operation))
                return "fixture-receipt"

        outbox = ChannelOutbox(repo)
        for index, route in enumerate(routes):
            with pytest.raises(ContractError):
                ChannelDelivery(outbox, route, Transport()).deliver(
                    events[1 - index].operation_id, "wrong-route")
            ChannelDelivery(outbox, route, Transport()).deliver(events[index].operation_id, "claim")
            assert runtimes[index].step() is None
        assert len(calls) == 2 and len(sent) == 2
        assert [value[1] for value in sent] == [{"output_text": f"answer-{i}"} for i in range(2)]
    finally:
        for runtime in reversed(runtimes):
            runtime.close()
    reopened = RuntimeRepository(repo.root)
    for index, route in enumerate(routes):
        rows = ChannelOutbox(reopened).read(route)
        assert len(rows) == 1 and rows[0]["state"] == "sent"
        assert rows[0]["operation_id"] == events[index].operation_id
        assert RuntimeInbox(reopened).next_pending(route.session) is None
