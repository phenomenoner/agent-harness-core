"""Controlled channel -> admitted parent -> child mailbox -> durable result."""

import pytest
from test_runtime_core import setup

from ahc.adapters.unavailable_memory import UnavailableMemoryPort
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.child_dispatch import ChildDispatchService
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.child_work import ChildParent, ChildWorkEnvelope
from ahc.domain.memory_availability import MemoryAvailability
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict


def test_ingress_child_result_survives_reopen_without_parent_replay(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "account", "channel", "user", snapshot.session)
    inbox = RuntimeInbox(store)
    event = ChannelInboundEvent(route, "event", "do the work")
    receipt = ChannelIngress(inbox, route).submit(event, snapshot.goal.digest)

    class Tools:
        workspace = None

        def capabilities(self):
            return ()

    ingress = LocalRuntimeIngress(
        inbox,
        artifact_digest="a" * 64,
        configuration_digest=lambda _: "b" * 64,
        workspace_scope="workspace",
        clock=lambda: 100,
    )
    request = ingress(snapshot, Tools())
    assert request.operation_id == receipt["operation_id"]
    memory = UnavailableMemoryPort(MemoryAvailability("absent"))
    child_calls = []

    class Child:
        def run(self, envelope, *, cancelled):
            assert not cancelled()
            child_calls.append(envelope.child_id)
            return "child evidence"

        def close(self):
            child_calls.append("closed")

    class Backend:
        calls = 0
        parent = None

        def invoke(self, request):
            self.calls += 1
            self.parent = ChildParent(
                request.session, request.operation_id, request_fingerprint(request)
            )
            assert memory.read().status == "absent"
            children = ChildDispatchRepository(
                store, request, clock=lambda: 100, monotonic_clock=lambda: 0
            )
            dispatched = ChildDispatchService(children, lambda _: Child()).dispatch(
                ChildWorkEnvelope("child", self.parent, "subtask", "workspace", 200), "token"
            )
            assert dispatched["state"] == "terminal"
            assert store.intents(request.session) == ()
            return AgentTurnObservation(
                request, "completed", "controlled", 1, 1, output_text="parent result"
            )

    backend = Backend()
    result = RuntimeService(store, backend).execute(snapshot, request)
    assert result.result["output_text"] == "parent result"
    reopened = RuntimeRepository(store.root)
    mailbox = ChildWorkRepository(reopened, clock=lambda: 100).mailbox(backend.parent)
    assert mailbox[0]["result"] == {"output_text": "child evidence"}
    assert reopened.intents(request.session)[0]["result"]["output_text"] == "parent result"
    current = reopened.read_current(request.session.ref.virtual_session_id)
    assert RuntimeService(reopened, backend).execute(current, request) == result
    assert backend.calls == 1
    assert child_calls == ["child", "closed"]
    with pytest.raises(WorkingSetConflict):
        ChildWorkRepository(reopened).complete(backend.parent, "child", "token", "late")

    class Transport:
        calls = []

        def send(self, destination, payload, logical_delivery_id):
            self.calls.append((destination, payload, logical_delivery_id))
            return "controlled-receipt"

    transport = Transport()
    ChannelDelivery(ChannelOutbox(reopened), route, transport).deliver(
        request.operation_id, "delivery-token"
    )
    assert transport.calls == [(route, {"output_text": "parent result"}, request.operation_id)]
    fresh = RuntimeRepository(store.root)
    with pytest.raises(ContractError):
        ChannelDelivery(ChannelOutbox(fresh), route, transport).deliver(
            request.operation_id, "another-token"
        )
    assert len(transport.calls) == 1
