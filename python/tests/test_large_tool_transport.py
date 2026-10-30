"""Large tool telemetry must not discard a parent result or erase fault custody."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_outbox import state
from test_codex_client import setup
from test_codex_transport import _deadline, _transport

from ahc.adapters.codex_transport import CodexStdioTransport, CodexTransportError
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.runtime_service import RuntimeService
from ahc.domain.backend_diagnostics import diagnostic_projection
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_large_tool_event_preserves_subsequent_parent_final():
    event = {"method": "item/started", "params": {"item": {
        "type": "mcpToolCall", "id": "tool", "server": "fixture", "tool": "create_tree",
        "arguments": {"content": "x" * 6_541_197}}}}
    final = {"method": "turn/completed", "params": {"threadId": "parent",
              "turn": {"id": "turn", "status": "completed"}}}
    payload = b"".join(json.dumps(v).encode() + b"\n" for v in (event, final))
    transport, owner = _transport(payload)
    with transport:
        assert transport.receive(deadline=_deadline(3)) == event
        assert transport.receive(deadline=_deadline(3)) == final
    assert owner.cleanup_calls == [5]


def fault_result(tmp_path, *, detail=True):
    repo, snapshot, request, route = state(tmp_path)
    peer, client, _ = setup()
    request = replace(request, artifact_digest=client.executable_sha256,
                      configuration_digest=client.configuration_digest)
    original = peer.receive

    def receive(*, deadline):
        if len(peer.messages) == 1:
            kwargs = {"transport_failure": {
                "reason": "frame-size-limit", "observed_bytes": 16_777_217,
                "limit_bytes": 16_777_216, "payload": "PRIVATE-SENTINEL"}} if detail else {}
            raise CodexTransportError("PRIVATE-SENTINEL", transport_error_type="protocol-frame",
                                      **kwargs)
        return original(deadline=deadline)

    peer.receive = receive
    result = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
        snapshot, request)
    return repo, request, route, result


def test_transport_fault_survives_commit_notice_and_reopen(tmp_path):
    repo, request, route, result = fault_result(tmp_path)
    diagnostic = result.result["backend_diagnostic"]
    assert diagnostic["transport_failure"] == {
        "reason": "frame-size-limit", "observed_bytes": 16_777_217, "limit_bytes": 16_777_216}
    outbox = ChannelOutbox(RuntimeRepository(repo.root))
    text = outbox.read(route)[0]["payload"]["output_text"]
    assert "單筆通訊資料超過接收大小上限" in text
    assert text.index("中斷原因") < text.index("任務進度與操作紀錄已保留")
    assert "PRIVATE" not in json.dumps(result.result) + text
    assert result.result["disposition"] == "unknown" and result.result["unresolved_effects"]
    calls = []
    transport = SimpleNamespace(send=lambda *args: calls.append(args) or "receipt")
    assert ChannelDelivery(outbox, route, transport).deliver(request.operation_id, "claim")[
        "state"] == "sent"
    assert RuntimeRepository(repo.root).read_turn(route.session, request.operation_id) == result
    with pytest.raises(ContractError):
        ChannelDelivery(outbox, route, transport).deliver(request.operation_id, "again")
    assert len(calls) == 1


@pytest.mark.parametrize("delivery_state", ["pending", "claimed", "sent", "unknown"])
@pytest.mark.parametrize("quoted", [False, True])
def test_predecessor_transport_notice_retains_exact_bytes(tmp_path, delivery_state, quoted):
    repo, request, route, _ = fault_result(tmp_path, detail=False)
    old = ("⚠ 任務未完成\n這次未能取得模型的最終回報，因此先停止這回合。"
           "任務進度與操作紀錄已保留。\n部分操作可能已執行，結果尚未確認；不會自動重跑。"
           "\n建議下一步：先核對已保存的進度與原操作結果，再接續未完成部分；"
           "可用 /status 查看狀態。")
    payload = {"output_text": old}
    if quoted:
        from ahc.domain.channel_ingress import ChannelInboundEvent
        from ahc.infrastructure.channel_context_store import bind_context

        # Add the original-route context before storing its predecessor quoted notice.
        event = ChannelInboundEvent(route, "event", "hello", context={"message_id": "100"})
        repo._mutate(lambda db: bind_context(db, event))
        payload["reply_to_message_id"] = "100"
    encoded = canonical_json(payload).decode()
    repo._mutate(lambda db: db.execute(
        "UPDATE runtime_channel_outbox SET payload_json=?,state=?,claim_token=?,receipt_json=? "
        "WHERE operation_id=?", (encoded, delivery_state,
            "claim" if delivery_state == "claimed" else None,
            '"old-receipt"' if delivery_state == "sent" else None, request.operation_id)))
    reopened = ChannelOutbox(RuntimeRepository(repo.root))
    record = reopened.read(route)[0]
    assert record["payload"] == payload and record["state"] == delivery_state
    if delivery_state != "pending":
        with pytest.raises(ContractError):
            reopened.claim(route, request.operation_id, "again")


@pytest.mark.parametrize("reason", ["PRIVATE", 123, None, "frame-size-limit"])
def test_optional_transport_metadata_is_payload_free(reason):
    base = {"category": "transport", "phase": "receive", "last_method": "other",
            "code": "protocol-frame", "remaining_ms": 10, "frames": 5,
            "cleanup": "not-observed"}
    projected = diagnostic_projection({**base, "transport_failure": {
        "reason": reason, "observed_bytes": True, "limit_bytes": -1,
        "payload": "PRIVATE-SENTINEL"}})["backend_diagnostic"]
    assert "PRIVATE" not in json.dumps(projected)
    assert projected["code"] == "protocol-frame"
    if reason == "frame-size-limit":
        assert projected["transport_failure"] == {"reason": reason}
    else:
        assert "transport_failure" not in projected


def test_frame_bound_and_queue_memory_remain_finite():
    transport, _ = _transport()
    try:
        assert CodexStdioTransport.MAX_FRAME == 16 * 1048576
        assert transport._frames.maxsize * transport.MAX_FRAME <= 32 * 1048576
        assert transport.MAX_OUTBOUND_FRAME == 1048576
    finally:
        transport.close()


def test_reader_reports_only_the_bounded_oversize_prefix_and_closes():
    transport, owner = _transport(b"x" * (CodexStdioTransport.MAX_FRAME + 2))
    with pytest.raises(CodexTransportError) as fault:
        transport.receive(deadline=_deadline(3))
    assert fault.value.transport_error_type == "protocol-frame"
    assert fault.value.transport_failure == {
        "reason": "frame-size-limit", "observed_bytes": CodexStdioTransport.MAX_FRAME + 1,
        "limit_bytes": CodexStdioTransport.MAX_FRAME}
    assert owner.cleanup_calls == [5] and transport._closed
    assert not transport._reader.is_alive()


def test_reader_fault_is_retained_when_cleanup_is_unverified():
    transport, owner = _transport(b"{}", complete=False, remaining_count=1)
    with pytest.raises(CodexTransportError) as fault:
        transport.receive(deadline=_deadline(3))
    assert fault.value.transport_error_type == "cleanup-unverified"
    assert fault.value.transport_failure == {
        "reason": "unterminated-frame", "observed_bytes": 2,
        "limit_bytes": CodexStdioTransport.MAX_FRAME}
    assert not transport._closed and owner.cleanup_calls == [5]
