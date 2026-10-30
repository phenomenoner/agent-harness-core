"""Provider notices identify the cause without losing existing delivery custody."""

import json
from dataclasses import replace

import pytest
from test_channel_outbox import state
from test_codex_client import setup as codex_setup

from ahc.adapters.channel_text import BotTextTransport
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def provider_failure(tmp_path, *, kind="overloaded", platform="telegram"):
    repo, snapshot, template, _ = state(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "overload", "continue the task",
                                context={"message_id": "100"})
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    peer, client, model_request = codex_setup()
    peer.messages.insert(3, {
        "method": "error", "params": {
            "threadId": "thread", "turnId": "turn", "willRetry": False,
            "error": {"message": "PRIVATE provider text",
                      "codexErrorInfo": kind},
        },
    })
    observation = client.run_admitted(model_request)

    class Backend:
        def invoke(self, bound):
            return AgentTurnObservation(
                bound, observation.disposition, "fixture", 1, 1, "thread", "turn",
                unresolved_effects=observation.unresolved_effects,
                backend_diagnostic=observation.backend_diagnostic,
            )

    result = RuntimeService(repo, Backend()).execute(snapshot, request)
    return repo, request, route, result


@pytest.mark.parametrize("kind,cause", [
    ("serverOverloaded", "模型供應商暫時過載"),
    ("flexUnavailable", "模型供應商暫時無可用容量"),
    ("rateLimitExceeded", "模型供應商限制請求頻率"),
    ("usageLimitExceeded", "模型供應商回報用量額度已達上限"),
])
@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_typed_provider_failure_reaches_original_platform_with_cause_first(
        tmp_path, kind, cause, platform):
    repo, request, route, result = provider_failure(tmp_path, kind=kind, platform=platform)
    text = ChannelOutbox(repo).read(route)[0]["payload"]["output_text"]
    assert cause in text
    assert text.index(cause) < text.index("任務進度與操作紀錄已保留")
    assert "不會自動重跑" in text and "/status" in text
    assert "PRIVATE" not in text
    if kind in {"serverOverloaded", "flexUnavailable"}:
        assert "稍後" in text and "其他模型" in text
        assert "用量限制" not in text and "額度已達上限" not in text
    calls = []

    def post(actual_platform, token, channel, body):
        assert (actual_platform, token, channel) == (platform, "fixture", "456")
        calls.append(body)
        text = body["text"] if platform == "telegram" else body["content"]
        assert cause in text
        if platform == "telegram":
            assert body["reply_parameters"]["message_id"] == 100
            return {"ok": True, "result": {
                "message_id": 1, "chat": {"id": 456}, "from": {"id": 123},
                "reply_to_message": {"message_id": 100, "chat": {"id": 456}}}}
        assert body["message_reference"]["message_id"] == "100"
        return {"id": "1", "channel_id": "456", "author": {"id": "123"},
                "message_reference": {"message_id": "100", "channel_id": "456"}}

    transport = BotTextTransport(route, "fixture", post=post)
    delivered = ChannelDelivery(ChannelOutbox(repo), route, transport).deliver(
        request.operation_id, "claim")
    assert delivered["state"] == "sent" and len(calls) == 1
    assert repo.read_turn(route.session, request.operation_id) == result
    assert not result.result["accepted"] and result.result["disposition"] == "unknown"
    assert result.result["unresolved_effects"]


@pytest.mark.parametrize("delivery_state", ["pending", "claimed", "sent", "unknown"])
@pytest.mark.parametrize("quoted", [False, True])
def test_preceding_capacity_notice_reopens_without_rewrite(tmp_path, delivery_state, quoted):
    repo, request, route, _ = provider_failure(tmp_path, kind="serverOverloaded")
    old_text = (
        "⚠ 任務未完成\n這次未能取得模型的最終回報，因此先停止這回合。"
        "任務進度與操作紀錄已保留。"
        "\n已收事件中尚未觀察到工具呼叫；結果仍未確認，不會自動重跑。"
        "\n建議下一步：先核對已保存的進度與原操作結果，再接續未完成部分；"
        "可用 /status 查看狀態。"
        "\n中斷原因：Codex 後端容量或用量限制，未取得最終回報。"
    )
    payload = {"output_text": old_text}
    if quoted:
        payload["reply_to_message_id"] = "100"
    encoded = canonical_json(payload).decode()
    outbox = ChannelOutbox(repo)
    if delivery_state == "claimed":
        outbox.claim(route, request.operation_id, "claim")
    elif delivery_state in {"sent", "unknown"}:
        class Destination:
            def send(self, *_):
                if delivery_state == "unknown":
                    raise OSError("lost acknowledgment")
                return "old-receipt"

        ChannelDelivery(outbox, route, Destination()).deliver(request.operation_id, "claim")
    repo._mutate(lambda db: db.execute(
        "UPDATE runtime_channel_outbox SET payload_json=? WHERE operation_id=?",
        (encoded, request.operation_id)))
    reopened = RuntimeRepository(repo.root)
    record = ChannelOutbox(reopened).read(route)[0]
    assert record["payload"] == payload and record["state"] == delivery_state
    with reopened._read() as db:
        assert db.execute(
            "SELECT payload_json FROM runtime_channel_outbox WHERE operation_id=?",
            (request.operation_id,)).fetchone()[0] == encoded


@pytest.mark.parametrize("kind", ["unauthorized", "other"])
def test_other_failures_are_not_labelled_as_provider_capacity(tmp_path, kind):
    repo, _, route, _ = provider_failure(tmp_path, kind=kind)
    text = ChannelOutbox(repo).read(route)[0]["payload"]["output_text"]
    assert "模型供應商暫時過載" not in text
    assert "PRIVATE" not in json.dumps(ChannelOutbox(repo).read(route))
