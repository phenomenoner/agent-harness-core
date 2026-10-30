"""Task-size and lifetime regressions at the core / channel / tool seams."""
import json
from dataclasses import replace

import pytest
from test_codex_client import setup as codex_setup
from test_codex_workspace_tools import bridge, message
from test_runtime_core import setup

from ahc.adapters.channel_text import BotTextTransport
from ahc.adapters.codex_transport import CodexStdioTransport
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.cli.foreground_learning import parse_learning
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_conversation_context import conversation_context
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("wrong", [False, True])
def test_full_large_result_becomes_one_document_without_losing_custody(tmp_path, platform, wrong):
    repo, snapshot, template = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "large", "Produce a complete useful report")
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(template, operation_id=event.operation_id,
                      goal_digest=snapshot.goal.digest, input_text=event.input_text)
    text = "完整資料🍐\n" * 70_000

    class Backend:
        def invoke(self, bound):
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                        output_text=text)

    result = RuntimeService(repo, Backend()).execute(snapshot, request)
    assert result.result["accepted"] and result.result["output_text"] == text
    calls = []

    def upload_post(bound, token, upload):
        calls.append(upload)
        assert bound == route and token == "fixture"
        assert upload.byte_count == len(text.encode("utf-8")) > 1_048_576
        assert text.encode("utf-8") in upload.body
        channel = "999" if wrong else "456"
        if platform == "telegram":
            return {"ok": True, "result": {
                "message_id": 1, "chat": {"id": int(channel)}, "from": {"id": 123},
                "document": {"file_id": "file", "file_name": upload.filename,
                             "file_size": upload.byte_count}}}
        return {"id": "1", "channel_id": channel, "author": {"id": "123"},
                "attachments": [{"id": "2", "filename": upload.filename,
                                 "size": upload.byte_count}]}

    def text_post(*args):
        pytest.fail("A long accepted answer must use its one document delivery")

    delivery = ChannelDelivery(ChannelOutbox(repo), route,
        BotTextTransport(route, "fixture", post=text_post, upload_post=upload_post))
    receipt = delivery.deliver(event.operation_id, "claim")
    assert receipt["state"] == ("unknown" if wrong else "sent")
    with pytest.raises(ContractError):
        delivery.deliver(event.operation_id, "second-claim")
    assert len(calls) == 1
    following = ChannelInboundEvent(route, "following", "Continue")
    ChannelIngress(RuntimeInbox(repo), route).submit(following, snapshot.goal.digest)
    history = conversation_context(repo, route.session, following.operation_id)
    assert history == ()  # An oversized context pair is skipped, not marked corrupt.


def test_large_observed_tool_feedback_is_partial_without_reexecuting():
    adapter, tools = bridge()
    calls = []
    value = {"disposition": "observed", "unresolved_effects": False,
             "result": {"ok": True, "output": "資料🍐\n" * 180_000}}
    tools.invoke = lambda name, args: calls.append((name, args)) or value
    response = adapter.respond(message())
    wire = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
    assert len(wire) <= CodexStdioTransport.MAX_OUTBOUND_FRAME
    projected = json.loads(response["result"]["contentItems"][0]["text"])
    assert projected["disposition"] == "observed" and projected["result"]["ok"]
    assert not projected["unresolved_effects"] and response["result"]["success"]
    assert projected["result_projection"]["truncated"] is True
    assert "do not rerun writes" in projected["result_projection"]["hint"]
    assert len(calls) == 1
    with pytest.raises(ContractError):
        adapter.respond(message())


def test_many_notices_before_admission_still_respect_the_deadline():
    peer, client, request = codex_setup()
    peer.messages[1:1] = [{"method": "remoteControl/status/changed", "params": {
        "installationId": "fixture", "serverName": "fixture", "status": "disabled"}}] * 5000
    original_receive = peer.receive

    def receive(*, deadline):
        result = original_receive(deadline=deadline)
        if client.frames >= 25:
            client.clock = lambda: request.deadline
        return result

    peer.receive = receive
    result = client.run_admitted(request)
    assert result.disposition == "not-submitted" and client.frames == 25
    assert not any(item.get("method") == "turn/start" for item in peer.sent)


def test_learning_respects_owner_budget_without_an_extra_parser_ceiling():
    result = parse_learning({"skill_root": "fixture", "learning": {
        "mode": "proposal-only", "protected_skills": [f"skill-{i}" for i in range(129)],
        "daily_limit": 1000}})
    assert len(result["learning"]["protected_skills"]) == 129
    assert result["learning"]["daily_limit"] == 1000
