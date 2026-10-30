"""Exact route references traverse real tool/client/result/outbox seams."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_codex_client import Peer
from test_runtime_core import setup as runtime_setup
from test_workspace_tools import Port

from ahc.adapters.channel_text import BotTextTransport
from ahc.adapters.codex_client import CodexProtocolClient
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.runtime_turn import accept_observation
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_reply_catalog import ChannelReplyTools, known_messages
from ahc.infrastructure.runtime_conversation_context import conversation_context
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt


def prepared(tmp_path, platform="telegram"):
    repo, snapshot, template = runtime_setup(tmp_path)
    route = ChannelRoute(platform, "42", "43", "44", snapshot.session)
    event = ChannelInboundEvent(route, "message-100", "please reply", context={
        "message_id": "100", "reply_to_id": "91", "quote": "original question",
        "quote_status": "available"})
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    tools = WorkspaceTools(workspace, artifacts=ArtifactRepository(repo),
                           channel_replies=ChannelReplyTools(repo))
    peer = Peer()
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture",
                                 clock=lambda: 0, workspace_tools=tools)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text,
                      configuration_digest=client.configuration_digest, workspace_scope="work",
                      capabilities=tools.capabilities(), deadline=100,
                      artifact_digest=client.executable_sha256)
    return repo, snapshot, route, workspace, tools, peer, client, request


def call(peer, workspace, tool, arguments, identifier=7):
    peer.messages.insert(3, {"id": identifier, "method": "item/tool/call", "params": {
        "threadId": "thread", "turnId": "turn", "namespace": "workspace",
        "tool": tool, "callId": "call-" + str(identifier),
        "arguments": {"expected": workspace.identity.to_dict(), **arguments}}})


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_native_reply_survives_actual_client_commit_reopen_and_provider_ack(tmp_path, platform):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path, platform)
    try:
        call(peer, workspace, "reply", {"message_id": "91"})
        call(peer, workspace, "channel_messages", {}, 8)
        turn = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
            snapshot, request)
        assert turn.result["accepted"] and turn.result["reply_to_message_id"] == "91"
        reopened = RuntimeRepository(repo.root)
        outbox = ChannelOutbox(reopened)
        assert outbox.read(route)[0]["payload"]["reply_to_message_id"] == "91"
        bodies = []

        def post(received_platform, token, channel, body):
            bodies.append(body)
            assert received_platform == platform and channel == "43"
            if platform == "telegram":
                assert body["reply_parameters"] == {
                    "message_id": 91, "allow_sending_without_reply": False}
                return {"ok": True, "result": {"message_id": 101, "chat": {"id": 43},
                    "from": {"id": 42}, "reply_to_message": {
                        "message_id": 91, "chat": {"id": 43}}}}
            assert body["message_reference"] == {"type": 0, "message_id": "91",
                "channel_id": "43", "fail_if_not_exists": True}
            assert body["allowed_mentions"] == {"parse": [], "replied_user": False}
            return {"id": "101", "channel_id": "43", "author": {"id": "42"},
                    "message_reference": {"message_id": "91", "channel_id": "43"}}

        sent = ChannelDelivery(outbox, route, BotTextTransport(route, "token", post=post)).deliver(
            request.operation_id, "send-once")
        assert sent["state"] == "sent" and len(bodies) == 1
        with reopened._read() as connection:
            messages = known_messages(connection, route.session, request.operation_id)
        assert any(m["kind"] == "assistant" and m["message_id"] == "101" for m in messages)
        next_event = ChannelInboundEvent(route, "message-102", "continue", context={
            "message_id": "102"})
        ChannelIngress(RuntimeInbox(reopened), route).submit(next_event, snapshot.goal.digest)
        history = conversation_context(reopened, route.session, next_event.operation_id)
        assert history[-1]["assistant"] == turn.result["output_text"]
    finally:
        workspace.close()


@pytest.mark.parametrize("target", ["999", "-1", "0", "1; other-channel", True])
def test_unknown_or_invalid_target_is_rejected_without_poisoning_answer(tmp_path, target):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path)
    try:
        call(peer, workspace, "reply", {"message_id": target})
        turn = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
            snapshot, request)
        assert turn.result["accepted"] and "reply_to_message_id" not in turn.result
        assert next(x for x in peer.sent if x.get("id") == 7)["result"]["success"] is False
        assert "reply_to_message_id" not in ChannelOutbox(repo).read(route)[0]["payload"]
    finally:
        workspace.close()


def test_reference_from_another_route_does_not_enter_catalog(tmp_path):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path)
    try:
        other = replace(route, channel_id="55")
        event = ChannelInboundEvent(other, "message-999", "private", context={"message_id": "999"})
        ChannelIngress(RuntimeInbox(repo), other).submit(event, snapshot.goal.digest)
        with repo._read() as c:
            assert "999" not in {m["message_id"] for m in known_messages(
                c, route.session, request.operation_id)}
        assert tools.invoke("workspace.reply", {"expected": workspace.identity.to_dict(),
                                               "message_id": "91"})["disposition"] == "rejected"
    finally:
        workspace.close()


def test_outbox_reply_tamper_does_not_bypass_answer_seal(tmp_path):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path)
    try:
        call(peer, workspace, "reply", {"message_id": "91"})
        RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(snapshot, request)
        payload = ChannelOutbox(repo).read(route)[0]["payload"]
        payload["reply_to_message_id"] = "100"
        repo._mutate(lambda c: c.execute(
            "UPDATE runtime_channel_outbox SET payload_json=? WHERE operation_id=?",
            (canonical_json(payload).decode(), request.operation_id)))
        with pytest.raises(WorkingSetCorrupt):
            ChannelOutbox(repo).read(route)
    finally:
        workspace.close()


def test_agent_can_clear_reply_before_final_commit(tmp_path):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path)
    try:
        call(peer, workspace, "reply", {"message_id": None}, 8)
        call(peer, workspace, "reply", {"message_id": "91"}, 7)
        turn = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
            snapshot, request)
        assert turn.result["accepted"] and "reply_to_message_id" not in turn.result
        assert "reply_to_message_id" not in ChannelOutbox(repo).read(route)[0]["payload"]
        assert all(x["result"]["success"] for x in peer.sent if x.get("id") in (7, 8))
    finally:
        workspace.close()


def test_unaccepted_observation_cannot_select_reply(tmp_path):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path)
    try:
        value = AgentTurnObservation(request, "not-submitted", "fixture", 0, 0,
                                     reply_to_message_id="91")
        with pytest.raises(ContractError):
            accept_observation(request, value)
    finally:
        workspace.close()


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_reply_receipt_mismatch_stays_unknown_without_fallback(tmp_path, platform):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path, platform)
    try:
        call(peer, workspace, "reply", {"message_id": "91"})
        RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(snapshot, request)
        calls = []

        def post(*args):
            calls.append(args[-1])
            return {"ok": True, "result": {"message_id": 101, "chat": {"id": 43},
                "from": {"id": 42}}, "id": "101", "channel_id": "43", "author": {"id": "42"}}

        delivery = ChannelDelivery(ChannelOutbox(repo), route,
                                   BotTextTransport(route, "token", post=post))
        assert delivery.deliver(request.operation_id, "once")["state"] == "unknown"
        with pytest.raises(ContractError):
            delivery.deliver(request.operation_id, "again")
        assert len(calls) == 1
    finally:
        workspace.close()
