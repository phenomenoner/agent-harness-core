"""Composed reply identity, multi-artifact delivery, and ambiguous outcome checks."""

import json
from types import SimpleNamespace

import pytest
from test_outbound_media_scenario import png
from test_outbound_replies import call, prepared

from ahc.adapters.channel_attachment import BotAttachmentTransport
from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
from ahc.application.runtime_service import RuntimeService
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.channel_reply_catalog import known_messages
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize(
    "fault", [None, "wrong-chat", "wrong-reply-id", "wrong-reply-chat", "timeout-after-effect"]
)
def test_photo_and_document_reply_preserve_route_provenance_and_unknown_state(tmp_path, fault):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path)
    try:
        photo = tools.artifacts.publish(workspace.identity, png(), "image/png")
        document = tools.artifacts.publish(workspace.identity, b"report", "text/plain")
        continuation = {}
        call(peer, workspace, "channel_messages", {}, 10)
        send = peer.send

        def queue_continuation(tool, arguments, identifier):
            peer.messages.insert(0, {"id": identifier, "method": "item/tool/call", "params": {
                "threadId": "thread", "turnId": "turn", "namespace": "workspace",
                "tool": tool, "callId": "call-" + str(identifier),
                "arguments": {"expected": workspace.identity.to_dict(), **arguments}}})

        def continue_after_catalog(message, *, deadline):
            send(message, deadline=deadline)
            if message.get("id") != 10:
                return
            catalog = json.loads(message["result"]["contentItems"][0]["text"])
            source = next(item for item in catalog["result"]["messages"]
                          if item["kind"] == "quoted" and item["message_id"] == "91")
            continuation.update(catalog=catalog, target=source["message_id"],
                                acknowledgement=message)
            # The model's next tool calls use the message identity returned by the
            # admitted route catalog, with the same current workspace identity.
            queue_continuation("attach", {"artifact_id": document.artifact_id,
                 "filename": "report.txt", "delivery_mode": "document",
                 "caption": "document caption"}, 11)
            queue_continuation("attach", {"artifact_id": photo.artifact_id,
                 "filename": "red.png", "delivery_mode": "photo",
                 "caption": "photo caption"}, 12)
            queue_continuation("reply", {"message_id": continuation["target"]}, 13)

        peer.send = continue_after_catalog

        turn = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
            snapshot, request)
        reply_target = continuation["target"]
        catalog_ack = continuation["acknowledgement"]
        assert catalog_ack["result"]["success"] is True
        assert continuation["catalog"]["disposition"] == "observed"
        assert turn.result["accepted"]
        assert turn.result["reply_to_message_id"] == reply_target
        assert {item["artifact_id"] for item in turn.result["artifacts"]} == {
            photo.artifact_id, document.artifact_id,
        }

        reopened = RuntimeRepository(repo.root)
        outbox = ChannelAttachmentOutbox(reopened)
        rows = outbox.read(route)
        assert len(rows) == 2
        assert all(row["reply_to_message_id"] == reply_target for row in rows)
        selected = {row["selection"]["filename"]: row["selection"] for row in rows}
        assert selected["red.png"]["artifact_id"] == photo.artifact_id
        assert selected["report.txt"]["artifact_id"] == document.artifact_id
        assert {value.get("delivery_mode", "document") for value in selected.values()} == {"photo", "document"}

        attempts, possible_effects = [], []

        def post(bound, token, upload):
            assert bound == route and token == "token"
            assert upload.reply_to_message_id == reply_target
            assert b'name="reply_parameters"' in upload.body
            assert f'"message_id": {int(reply_target)}'.encode() in upload.body
            attempts.append(upload)
            possible_effects.append(upload)
            second_attempt = len(attempts) == 2
            reply_id = int(reply_target) + int(second_attempt and fault == "wrong-reply-id")
            reply_chat = 999 if second_attempt and fault == "wrong-reply-chat" else 43
            message_chat = 999 if second_attempt and fault == "wrong-chat" else 43
            message = {
                "message_id": 100 + len(attempts),
                "chat": {"id": message_chat},
                "from": {"id": 42},
                "reply_to_message": {"message_id": reply_id, "chat": {"id": reply_chat}},
            }
            if upload.delivery_mode == "photo":
                message["photo"] = [{"file_id": "photo-1", "width": 1, "height": 1,
                                     "file_size": 150}]
            else:
                message["document"] = {"file_id": "document-1", "file_name": upload.filename,
                                        "file_size": upload.byte_count}
            if second_attempt and fault == "timeout-after-effect":
                raise TimeoutError("fixture timeout after possible remote effect")
            return {"ok": True, "result": message}

        delivery = ChannelAttachmentDelivery(
            outbox, route, BotAttachmentTransport(route, "token", post=post)
        )
        for row in rows:
            delivered = delivery.deliver(
                request.operation_id, row["ordinal"], "claim-" + str(row["ordinal"])
            )
            expected = "sent" if fault is None or row["ordinal"] == 0 else "unknown"
            assert delivered["state"] == expected

        assert len(attempts) == len(possible_effects) == 2
        reopened_rows = outbox.read(route)
        assert [row["state"] for row in reopened_rows] == (
            ["sent", "sent"] if fault is None else ["sent", "unknown"]
        )
        if fault is not None:
            with pytest.raises(ContractError):
                delivery.deliver(request.operation_id, 1, "retry-unknown")
            assert len(attempts) == 2

        with reopened._read() as connection:
            messages = known_messages(connection, route.session, request.operation_id)
        assistant_attachments = {
            message["message_id"]: message["excerpt"] for message in messages
            if message["kind"] == "assistant_attachment"
        }
        sent_receipts = {
            json.loads(row["receipt"])["message_id"]:
                (row["selection"].get("caption") or row["selection"]["filename"])
            for row in reopened_rows if row["state"] == "sent"
        }
        assert assistant_attachments == sent_receipts
    finally:
        workspace.close()
