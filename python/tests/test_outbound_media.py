"""Focused outbound photo/document presentation and provider receipt checks."""

import json
import zlib
from email import policy
from email.parser import BytesParser

import pytest
from test_workspace_tools import setup

from ahc.adapters.channel_attachment import BotAttachmentTransport
from ahc.application.channel_artifact_upload import prepare_artifact_upload
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.channel_photo import validate_photo
from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository


def _png(width=100, height=100):
    def chunk(kind, data):
        return (len(data).to_bytes(4, "big") + kind + data
                + (zlib.crc32(kind + data) & 0xFFFFFFFF).to_bytes(4, "big"))

    header = (width.to_bytes(4, "big") + height.to_bytes(4, "big")
              + b"\x08\x02\x00\x00\x00")
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header)
            + chunk(b"IDAT", zlib.compress(b"")) + chunk(b"IEND", b""))


def _jpeg(width=100, height=100):
    return (b"\xff\xd8\xff\xc0" + (11).to_bytes(2, "big") + b"\x08"
            + height.to_bytes(2, "big") + width.to_bytes(2, "big")
            + b"\x01\x01\x11\x00" + b"\xff\xd9")


@pytest.mark.parametrize("factory,media", [(_png, "image/png"), (_jpeg, "image/jpeg")])
def test_photo_rejects_sum_of_dimensions_above_provider_limit(factory, media):
    with pytest.raises(ContractError):
        validate_photo(factory(6000, 6000), media)


def _corrupt_png():
    content = bytearray(_png())
    content[29] ^= 1
    return bytes(content)


def _parts(upload):
    message = BytesParser(policy=policy.default).parsebytes(
        f"Content-Type: {upload.content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode()
        + upload.body
    )
    return {part.get_param("name", header="content-disposition"): part
            for part in message.iter_parts()}


def _route(workspace, platform):
    return ChannelRoute(platform, "123", "456", "789", workspace.identity.session)


def test_result_artifact_defaults_and_optional_fields_are_strict():
    artifact_id = "a" * 64
    legacy = ResultArtifact(artifact_id, "report.bin")
    assert legacy.to_dict() == {"artifact_id": artifact_id, "filename": "report.bin"}
    assert ResultArtifact.from_dict(legacy.to_dict()) == legacy

    photo = ResultArtifact(artifact_id, "photo.png", "photo", "caption 😀")
    assert photo.to_dict() == {
        "artifact_id": artifact_id,
        "filename": "photo.png",
        "delivery_mode": "photo",
        "caption": "caption 😀",
    }
    assert ResultArtifact.from_dict(photo.to_dict()) == photo
    for value in (
        {"artifact_id": artifact_id},
        {"artifact_id": artifact_id, "filename": "photo.png", "unknown": True},
    ):
        with pytest.raises(ContractError):
            ResultArtifact.from_dict(value)
    with pytest.raises(ContractError):
        ResultArtifact(artifact_id, "photo.png", "audio")
    with pytest.raises(ContractError):
        ResultArtifact(artifact_id, "photo.png", caption="😀" * 1024)


@pytest.mark.parametrize("media_type,content,dimensions", [
    ("image/png", _png(200, 100), (200, 100)),
    ("image/jpeg", _jpeg(320, 240), (320, 240)),
])
def test_validate_photo_accepts_verified_bounded_png_and_jpeg(media_type, content, dimensions):
    assert validate_photo(content, media_type) == dimensions


@pytest.mark.parametrize("media_type,content", [
    ("image/gif", b"GIF89a"),
    ("image/png", b"not-a-png"),
    ("image/png", _png()[:-1]),
    ("image/png", _corrupt_png()),
    ("image/jpeg", b"\xff\xd8\xff\xd9"),
    ("image/jpeg", _jpeg()[:-2]),
    ("text/plain", _png()),
])
def test_validate_photo_rejects_unverified_media(media_type, content):
    with pytest.raises(ContractError, match="photo content invalid"):
        validate_photo(content, media_type)


def test_telegram_photo_multipart_and_reencoded_receipt(tmp_path):
    store, workspace, _ = setup(tmp_path)
    try:
        content = _png(100, 80)
        manifest = ArtifactRepository(store).publish(workspace.identity, content, "image/png")
        route = _route(workspace, "telegram")
        upload = prepare_artifact_upload(
            store, route, manifest.artifact_id, "source.png", delivery_mode="photo",
            caption="圖片 😀", reply_to_message_id="77",
        )
        parts = _parts(upload)
        assert parts["photo"].get_payload(decode=True) == content
        assert parts["caption"].get_payload(decode=True).decode() == "圖片 😀"
        assert json.loads(parts["reply_parameters"].get_payload(decode=True)) == {
            "message_id": 77, "allow_sending_without_reply": False,
        }
        calls = []

        def post(bound, token, candidate):
            calls.append(candidate)
            assert bound == route and token == "fixture"
            return {"ok": True, "result": {
                "message_id": 101,
                "chat": {"id": 456},
                "from": {"id": 123},
                "reply_to_message": {"message_id": 77, "chat": {"id": 456}},
                "photo": [
                    {"file_id": "small", "width": 100, "height": 80, "file_size": 400},
                    {"file_id": "large", "width": 800, "height": 640, "file_size": 2_000_000},
                ],
            }}

        receipt = BotAttachmentTransport(route, "fixture", post=post).send(upload)
        assert receipt == {
            "artifact_id": manifest.artifact_id,
            "delivery_mode": "photo",
            "source_byte_count": len(content),
            "message_id": "101",
            "attachment_id": "large",
        }
        assert len(calls) == 1
    finally:
        workspace.close()


def test_telegram_document_legacy_body_and_receipt_remain_unchanged(tmp_path):
    store, workspace, _ = setup(tmp_path)
    try:
        content = b"report"
        manifest = ArtifactRepository(store).publish(workspace.identity, content, "text/plain")
        route = _route(workspace, "telegram")
        upload = prepare_artifact_upload(store, route, manifest.artifact_id, "report.txt")
        parts = _parts(upload)
        assert set(parts) == {"chat_id", "disable_content_type_detection", "document"}
        calls = []

        def post(bound, token, candidate):
            calls.append(candidate)
            return {"ok": True, "result": {
                "message_id": 102, "chat": {"id": 456}, "from": {"id": 123},
                "document": {"file_id": "doc-1", "file_name": "report.txt",
                              "file_size": len(content)},
            }}

        receipt = BotAttachmentTransport(route, "fixture", post=post).send(upload)
        assert receipt == {"artifact_id": manifest.artifact_id, "message_id": "102",
                           "attachment_id": "doc-1", "filename": "report.txt",
                           "byte_count": len(content)}
        assert len(calls) == 1
    finally:
        workspace.close()


def test_discord_photo_caption_reply_and_source_correlation(tmp_path):
    store, workspace, _ = setup(tmp_path)
    try:
        content = _jpeg(160, 120)
        manifest = ArtifactRepository(store).publish(workspace.identity, content, "image/jpeg")
        route = _route(workspace, "discord")
        upload = prepare_artifact_upload(
            store, route, manifest.artifact_id, "source.jpg", delivery_mode="photo",
            caption="caption", reply_to_message_id="77",
        )
        metadata = json.loads(_parts(upload)["payload_json"].get_payload(decode=True))
        assert metadata["content"] == "caption"
        assert metadata["attachments"] == [{"id": 0, "filename": "source.jpg"}]
        assert metadata["message_reference"] == {
            "type": 0, "message_id": "77", "channel_id": "456", "fail_if_not_exists": True,
        }
        assert metadata["allowed_mentions"] == {"parse": [], "replied_user": False}
        calls = []

        def post(bound, token, candidate):
            calls.append(candidate)
            return {"id": "900", "channel_id": "456", "author": {"id": "123"},
                    "message_reference": {"type": 0, "message_id": "77", "channel_id": "456"},
                    "attachments": [{"id": "901", "filename": "source.jpg",
                                     "size": len(content)}]}

        receipt = BotAttachmentTransport(route, "fixture", post=post).send(upload)
        assert receipt == {
            "artifact_id": manifest.artifact_id,
            "message_id": "900",
            "attachment_id": "901",
            "filename": "source.jpg",
            "byte_count": len(content),
            "delivery_mode": "photo",
            "source_byte_count": len(content),
        }
        assert len(calls) == 1
    finally:
        workspace.close()


def test_photo_preparation_rejects_invalid_media_and_caption_before_transport(tmp_path):
    store, workspace, _ = setup(tmp_path)
    try:
        route = _route(workspace, "telegram")
        invalid = ArtifactRepository(store).publish(workspace.identity, b"not-image", "image/png")
        with pytest.raises(ContractError, match="photo content invalid"):
            prepare_artifact_upload(store, route, invalid.artifact_id, "bad.png",
                                    delivery_mode="photo")
        valid = ArtifactRepository(store).publish(workspace.identity, _png(), "image/png")
        with pytest.raises(ContractError, match="caption invalid"):
            prepare_artifact_upload(store, route, valid.artifact_id, "ok.png",
                                    delivery_mode="photo", caption="x" * 1025)
        with pytest.raises(ContractError):
            prepare_artifact_upload(store, route, valid.artifact_id, "ok.png",
                                    delivery_mode="photo", reply_to_message_id="0")
    finally:
        workspace.close()


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_foreign_reply_receipt_is_rejected_without_retry(tmp_path, platform):
    store, workspace, _ = setup(tmp_path)
    try:
        content = _png()
        manifest = ArtifactRepository(store).publish(workspace.identity, content, "image/png")
        route = _route(workspace, platform)
        upload = prepare_artifact_upload(store, route, manifest.artifact_id, "photo.png",
                                         delivery_mode="photo", reply_to_message_id="77")
        calls = []

        def post(bound, token, candidate):
            calls.append(candidate)
            if platform == "telegram":
                return {"ok": True, "result": {
                    "message_id": 103, "chat": {"id": 456}, "from": {"id": 123},
                    "reply_to_message": {"message_id": 77, "chat": {"id": 999}},
                    "photo": [{"file_id": "photo", "width": 100, "height": 100,
                               "file_size": 100}],
                }}
            return {"id": "902", "channel_id": "456", "author": {"id": "123"},
                    "message_reference": {"type": 0, "message_id": "77", "channel_id": "999"},
                    "attachments": [{"id": "903", "filename": "photo.png",
                                     "size": len(content)}]}

        with pytest.raises(ContractError, match="response unverified"):
            BotAttachmentTransport(route, "fixture", post=post).send(upload)
        assert len(calls) == 1
    finally:
        workspace.close()
