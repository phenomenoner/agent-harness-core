"""Agent tool selections reach durable text/photo/document delivery after reopen."""

import struct
import zlib
from types import SimpleNamespace

import pytest
from test_outbound_replies import call, prepared

from ahc.adapters.channel_attachment import BotAttachmentTransport
from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.runtime_service import RuntimeService
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_reply_catalog import known_messages
from ahc.infrastructure.runtime_repository import RuntimeRepository


def png():
    def chunk(name, data):
        return (struct.pack(">I", len(data)) + name + data
                + struct.pack(">I", zlib.crc32(name + data)))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(b"\0\xff\0\0")) + chunk(b"IEND", b""))


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_agent_reply_photo_document_commit_reopen_order_and_catalog(tmp_path, platform):
    repo, snapshot, route, workspace, tools, peer, client, request = prepared(tmp_path, platform)
    try:
        photo = tools.artifacts.publish(workspace.identity, png(), "image/png")
        document = tools.artifacts.publish(workspace.identity, b"report", "text/plain")
        call(peer, workspace, "reply", {"message_id": "91"}, 7)
        call(peer, workspace, "attach", {"artifact_id": photo.artifact_id, "filename": "red.png",
             "delivery_mode": "photo", "caption": "照片說明"}, 8)
        call(peer, workspace, "attach", {"artifact_id": document.artifact_id,
             "filename": "report.txt", "delivery_mode": "document", "caption": "文件說明"}, 9)
        turn = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
            snapshot, request)
        assert turn.result["accepted"] and turn.result["reply_to_message_id"] == "91"
        assert len(turn.result["artifacts"]) == 2
        reopened = RuntimeRepository(repo.root)
        text_calls = []

        def send(bound, payload, operation):
            assert bound == route and payload["reply_to_message_id"] == "91"
            text_calls.append(operation)
            return f"{platform}:43:101"

        assert ChannelDelivery(ChannelOutbox(reopened), route,
                               SimpleNamespace(send=send)).deliver(
                                   request.operation_id, "text")["state"] == "sent"
        uploads = []

        def post(bound, token, upload):
            assert bound == route and upload.reply_to_message_id == "91"
            uploads.append(upload)
            identifier = 102 + len(uploads)
            if platform == "telegram":
                message = {"message_id": identifier, "chat": {"id": 43}, "from": {"id": 42},
                           "reply_to_message": {"message_id": 91, "chat": {"id": 43}}}
                if upload.delivery_mode == "photo":
                    assert b'name="photo"' in upload.body and "照片說明".encode() in upload.body
                    message["photo"] = [{"file_id": "photo-1", "file_unique_id": "unique-1",
                        "width": 1, "height": 1, "file_size": 150}]
                else:
                    assert b'name="document"' in upload.body
                    message["document"] = {"file_id": "document-1",
                        "file_name": upload.filename, "file_size": upload.byte_count}
                return {"ok": True, "result": message}
            assert b'"replied_user": false' in upload.body
            return {"id": str(identifier), "channel_id": "43", "author": {"id": "42"},
                "message_reference": {"message_id": "91", "channel_id": "43"},
                "attachments": [{"id": str(identifier + 10), "filename": upload.filename,
                                 "size": upload.byte_count}]}

        outbox = ChannelAttachmentOutbox(reopened)
        delivery = ChannelAttachmentDelivery(outbox, route,
                                             BotAttachmentTransport(route, "token", post=post))
        rows = outbox.read(route)
        assert len(rows) == 2 and all(row["reply_to_message_id"] == "91" for row in rows)
        for row in rows:
            assert delivery.deliver(request.operation_id, row["ordinal"],
                                    "file-" + str(row["ordinal"]))["state"] == "sent"
        assert text_calls == [request.operation_id]
        assert [upload.delivery_mode for upload in uploads] == ["document", "photo"]
        with reopened._read() as connection:
            messages = known_messages(connection, route.session, request.operation_id)
        assert len([m for m in messages if m["kind"] == "assistant_attachment"]) == 2
    finally:
        workspace.close()
