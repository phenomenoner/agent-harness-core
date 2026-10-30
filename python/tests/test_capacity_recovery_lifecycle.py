"""Controlled capacity fault with real Linux kernel, receipts and next-task use."""

import json
import sys
import time
from dataclasses import replace

import pytest
from test_codex_capacity_recovery import Clock, acknowledgement, completed, failed
from test_codex_client import Peer
from test_graceful_budget_lifecycle import call, client_and_request
from test_linux_native import native_owner as native_owner
from test_runtime_core import setup

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.adapters.native_python_workspace import NativePythonWorkspace
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.runtime_turn import accept_observation
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native Linux process lifecycle")


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("continuations", [1, 2])
@pytest.mark.parametrize("problem_report", [False, True])
@pytest.mark.usefixtures("native_owner")
def test_capacity_continuation_preserves_single_effect_delivery_and_next_task(
        tmp_path, platform, continuations, problem_report):
    repository, snapshot, template = setup(tmp_path)
    runner = NativeProcessRunner(cwd=str(tmp_path), environment={}, persistent=True,
                                 owner_lifetime=True)
    policy = EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 30)

    def create(identity, checkpoint):
        return NativePythonWorkspace(repository, runner, identity, effective_policy=policy,
            environment_digest="a" * 64, cwd=str(tmp_path), interpreter=sys.executable,
            user=None, timeout=10, lifetime_seconds=1200, engine="python", checkpoint=checkpoint)

    workspace = RuntimeService(repository, None).open_workspace(snapshot, "work", create)
    tools = WorkspaceTools(workspace)
    route = ChannelRoute(platform, "account", "channel", "user", snapshot.session)
    clock = Clock()
    clock.now = time.monotonic()
    effects, sent = [], []

    class ModelPeer(Peer):
        def send(self, message, **kwargs):
            super().send(message, **kwargs)
            if "result" in message and message.get("id") in {7, 8, 9}:
                value = json.loads(message["result"]["contentItems"][0]["text"])
                effects.append((message["id"], value))
                if message["id"] == 7:
                    self.messages.insert(0, call(8, "checkpoint", workspace,
                                                names=["capacity_count"]))
            if message.get("id") == 2_000_001 and continuations == 2:
                self.messages[:0] = [acknowledgement(1, "first"), failed("first")]
            elif message.get("id") == 2_000_000 + continuations:
                read = call(9, "execute", workspace,
                    code=("import os; os._exit(23)" if problem_report
                          else "assert capacity_count==1; print(capacity_count)"))
                read["params"]["turnId"] = "continued"
                self.messages[:0] = [acknowledgement(continuations, "continued"),
                                     read, completed("continued")]

    class Destination:
        def send(self, actual, payload, operation):
            assert actual == route
            sent.append((operation, payload["output_text"]))
            return "controlled-receipt-" + str(len(sent))

    peer = ModelPeer()
    peer.messages[3:] = [call(7, "execute", workspace,
        code="capacity_count=globals().get('capacity_count',0)+1; print(capacity_count)"), failed()]
    event = ChannelInboundEvent(route, "capacity-task", "perform one effect and report")
    ChannelIngress(RuntimeInbox(repository), route).submit(event, template.goal_digest)
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", workspace_tools=tools,
        clock=clock, recovery_wait=clock.wait, overload_recovery={"delays_seconds": [120, 300]})
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text,
        workspace_scope="work", deadline=clock.now + 1000,
        artifact_digest=client.executable_sha256, configuration_digest=client.configuration_digest,
        capabilities=tools.capabilities())
    client.answer_seal = lambda observation: repository.seal_observation(
        request, accept_observation(request, observation))

    class Backend:
        def invoke(self, admitted):
            with workspace.turn_scope(admitted.deadline):
                return client.run_admitted(admitted)

    try:
        result = RuntimeService(repository, Backend()).execute(snapshot, request)
        assert result.result["accepted"] is (not problem_report)
        assert result.result["unresolved_effects"] is problem_report
        assert result.result.get("report_accepted", False) is problem_report
        assert result.result["backend_recovery"] == {
            "attempts": continuations, "state": "recovered", "last_failure": "overloaded"}
        assert [i for i, _ in effects] == [7, 8, 9]
        assert effects[1][1]["result"]["digest"]
        outbox = ChannelOutbox(repository)
        assert ChannelDelivery(outbox, route, Destination()).deliver(
            request.operation_id, "capacity-result")["state"] == "sent"
        if problem_report:
            reopened = RuntimeRepository(repository.root)
            assert reopened.read_turn(route.session, request.operation_id) == result
            assert [operation for operation, _ in sent] == [request.operation_id]
            return
        current = repository.read_current(snapshot.session.ref.virtual_session_id)
        following_peer = Peer()
        following_peer.messages.insert(3, call(7, "execute", workspace,
            code="assert capacity_count==1; print('same effect, next task')"))
        following_event = ChannelInboundEvent(route, "after-capacity", "inspect saved progress")
        backend, following_request = client_and_request(
            repository, workspace, tools, following_peer, template, following_event)
        following = RuntimeService(repository, backend).execute(current, following_request)
        assert following.result["accepted"]
        assert ChannelDelivery(outbox, route, Destination()).deliver(
            following_request.operation_id, "next-result")["state"] == "sent"
        reopened = RuntimeRepository(repository.root)
        assert reopened.read_turn(route.session, request.operation_id) == result
        assert [operation for operation, _ in sent] == [
            request.operation_id, following_request.operation_id]
    finally:
        workspace.close()
    assert workspace.closed
