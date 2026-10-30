"""Controlled provider advice with real transport and SQLite/kernel/route continuation."""

import json
import time
import sys
from dataclasses import replace

import pytest
from test_codex_client import Peer, setup as protocol_setup
from test_codex_transport import _FakeOwned, _PacedStdout
from test_runtime_core import setup
from test_linux_native import native_owner

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_transport import CodexStdioTransport
from ahc.adapters.native_python_workspace import NativePythonWorkspace
from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def call(index, operation, workspace, **arguments):
    return {"id": index, "method": "item/tool/call", "params": {
        "threadId": "thread", "turnId": "turn", "namespace": "workspace",
        "tool": operation, "callId": "call-"+str(index), "arguments": {
            "expected": workspace.identity.to_dict(), **arguments}}}


def client_and_request(repository, workspace, tools, peer, template, event):
    ChannelIngress(RuntimeInbox(repository), event.route).submit(event, template.goal_digest)
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", workspace_tools=tools)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text,
        workspace_scope="work", deadline=time.monotonic()+60,
        artifact_digest=client.executable_sha256, configuration_digest=client.configuration_digest,
        capabilities=tools.capabilities())

    class Backend:
        def invoke(self, bound):
            with workspace.turn_scope(bound.deadline):
                return client.run_admitted(bound)

    return Backend(), request


def test_quiet_real_transport_delivers_warning_then_accepts_original_terminal():
    owned = _FakeOwned()
    stream = _PacedStdout()
    owned.process.stdout = stream
    messages = Peer().messages
    sends = []
    original_write = owned.process.stdin.write

    def emit(message):
        stream.frames.put((json.dumps(message) + "\n").encode())

    def write(raw):
        result = original_write(raw)
        message = json.loads(raw)
        sends.append(message)
        if message.get("method") in {"initialize", "thread/start", "turn/start"}:
            emit(messages.pop(0))
        elif message.get("method") == "turn/steer":
            assert message["params"]["threadId"] == "thread"
            assert message["params"]["expectedTurnId"] == "turn"
            emit({"id": message["id"], "result": {"turnId": "turn"}})
            emit(messages.pop(0))
        return result

    owned.process.stdin.write = write
    transport = CodexStdioTransport(owned)
    _, _, template = protocol_setup()
    client = CodexProtocolClient(transport, cwd="workspace", model="fixture", wrap_up_timeout=2)
    request = replace(template, deadline=time.monotonic() + 8,
                      configuration_digest=client.configuration_digest)
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert [m["method"] for m in sends if m.get("method") == "turn/start"] == ["turn/start"]
    assert len([m for m in sends if m.get("method") == "turn/steer"]) == 1
    assert not any(m.get("method") == "turn/interrupt" for m in sends)
    assert client._wrap_up.status == "accepted"
    assert transport.telemetry_snapshot()["closed"]
    assert owned.cleanup_calls == [5]


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_wrap_up_checkpoint_handover_delivery_and_next_task(native_owner, tmp_path, platform):
    repository, snapshot, template = setup(tmp_path)
    runner = NativeProcessRunner(cwd=str(tmp_path), environment={}, persistent=True,
                                 owner_lifetime=True)
    policy = EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 30)

    def create(identity, checkpoint):
        return NativePythonWorkspace(repository, runner, identity, effective_policy=policy,
            environment_digest="a" * 64, cwd=str(tmp_path), interpreter=sys.executable,
            user=None, timeout=10, lifetime_seconds=120, engine="python", checkpoint=checkpoint)

    workspace = RuntimeService(repository, None).open_workspace(snapshot, "work", create)
    tools = WorkspaceTools(workspace)
    route = ChannelRoute(platform, "account", "channel", "user", snapshot.session)
    handover = "Progress saved. Remaining: final rollout. Next: resume from the checkpoint."
    feedback, sends = [], []
    origin = time.monotonic()

    class Clock:
        value = origin
        def __call__(self):
            return self.value

    clock = Clock()

    class ModelPeer(Peer):
        quiet = False
        def receive(self, *, deadline, on_idle=None):
            if len(self.messages) == 1 and not self.quiet:
                self.quiet = True
                clock.value = origin + 95
                assert on_idle is not None
                on_idle()
            return super().receive(deadline=deadline)

        def send(self, message, **kwargs):
            super().send(message, **kwargs)
            if message.get("method") == "turn/steer":
                assert message["params"]["expectedTurnId"] == "turn"
                self.messages.insert(0, call(7, "execute", workspace,
                    code="handover_progress='confirmed-step'; print(handover_progress)"))
            elif "result" in message and message.get("id") in {7, 8}:
                value = json.loads(message["result"]["contentItems"][0]["text"])
                feedback.append(value)
                if message["id"] == 7:
                    self.messages.insert(0, call(8, "checkpoint", workspace,
                                                names=["handover_progress"]))

    class Destination:
        def send(self, actual, payload, operation):
            assert actual == route
            sends.append((operation, payload["output_text"]))
            return "receipt-" + str(len(sends))

    peer = ModelPeer()
    peer.messages[-1]["params"]["turn"]["items"][0]["text"] = handover
    event = ChannelInboundEvent(route, "near-budget", "do work and report saved progress")
    ChannelIngress(RuntimeInbox(repository), route).submit(event, template.goal_digest)
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", workspace_tools=tools,
                                clock=clock, wrap_up_timeout=180)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text,
        workspace_scope="work", deadline=origin+100, artifact_digest=client.executable_sha256,
        configuration_digest=client.configuration_digest, capabilities=tools.capabilities())

    class Backend:
        def invoke(self, admitted):
            with workspace.turn_scope(admitted.deadline):
                return client.run_admitted(admitted)

    try:
        result = RuntimeService(repository, Backend()).execute(snapshot, request)
        assert result.result["accepted"] and not result.result["unresolved_effects"]
        assert feedback[-1]["disposition"] == "observed" and feedback[-1]["result"]["digest"]
        saved_revision = workspace.identity.revision
        assert saved_revision > 0
        assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1
        assert len([m for m in peer.sent if m.get("method") == "turn/steer"]) == 1
        outbox = ChannelOutbox(repository)
        assert ChannelDelivery(outbox, route, Destination()).deliver(
            request.operation_id, "handover-claim")["state"] == "sent"
        current = repository.read_current(snapshot.session.ref.virtual_session_id)
        assert current.status == "active" and current.active_turn_id is None

        following_peer = Peer()
        following_peer.messages.insert(3, call(7, "execute", workspace,
            code="assert handover_progress=='confirmed-step'; print(handover_progress)"))
        following_event = ChannelInboundEvent(route, "after-budget", "continue saved work")
        backend, following_request = client_and_request(
            repository, workspace, tools, following_peer, template, following_event)
        following = RuntimeService(repository, backend).execute(current, following_request)
        assert following.result["accepted"] and workspace.identity.revision > saved_revision
        assert ChannelDelivery(outbox, route, Destination()).deliver(
            following_request.operation_id, "continuation-claim")["state"] == "sent"
        reopened = RuntimeRepository(repository.root)
        assert reopened.read_turn(route.session, request.operation_id) == result
        assert [operation for operation, _ in sends] == [
            request.operation_id, following_request.operation_id]
        assert sends[0][1].endswith(handover) and sends[1][1].endswith("answer")
    finally:
        workspace.close()
    assert workspace.closed
