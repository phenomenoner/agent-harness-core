"""Prepare a bounded artifact upload; preparation alone grants no delivery authority."""

import json
import hashlib
from dataclasses import dataclass, field
from uuid import uuid4

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.channel_photo import validate_photo
from ahc.domain.channel_presentation import text_document_caption
from ahc.domain.channel_reply import reply_message_id
from ahc.domain.result_artifacts import (
    display_filename,
    validate_caption,
    validate_delivery_mode,
)
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository


@dataclass(frozen=True, slots=True)
class ArtifactUpload:
    route: ChannelRoute
    artifact_id: str
    filename: str
    byte_count: int
    content_type: str
    body: bytes = field(repr=False)
    delivery_mode: str = "document"
    caption: str = ""
    reply_to_message_id: str | None = None


def prepare_artifact_upload(store, route, artifact_id, filename, *,
                            delivery_mode="document", caption="",
                            reply_to_message_id=None):
    if type(route) is not ChannelRoute:
        raise ContractError("artifact upload route invalid")
    display_filename(filename)
    validate_delivery_mode(delivery_mode)
    validate_caption(caption)
    if reply_to_message_id is not None:
        reply_to_message_id = reply_message_id(reply_to_message_id)
    current = store.read_current(route.session.ref.virtual_session_id)
    if current.session != route.session or current.status != "active":
        raise ContractError("artifact upload owner is not current and active")
    artifact = ArtifactRepository(store).read(route.session, artifact_id)
    if artifact is None:
        raise ContractError("artifact upload unavailable")
    manifest, content = artifact
    if delivery_mode == "photo":
        width, height = validate_photo(content, manifest.media_type)
        if route.platform == "telegram" and width + height > 10_000:
            raise ContractError("Telegram photo dimensions invalid")
    return _prepare_bytes_upload(route, artifact_id, filename, content,
                                 manifest.media_type, delivery_mode, caption,
                                 reply_to_message_id)


def prepare_text_upload(route, text, *, reply_to_message_id=None):
    """Format an already-claimed full text result as one document, without sending it."""
    if (type(route) is not ChannelRoute or type(text) is not str or not text
            or any(0xD800 <= ord(char) <= 0xDFFF for char in text)):
        raise ContractError("text document invalid")
    if reply_to_message_id is not None:
        reply_to_message_id = reply_message_id(reply_to_message_id)
    content = text.encode("utf-8")
    return _prepare_bytes_upload(route, hashlib.sha256(content).hexdigest(),
                                 "ahc-reply.txt", content, "text/plain; charset=utf-8",
                                 "document", text_document_caption(text), reply_to_message_id)


def _prepare_bytes_upload(route, artifact_id, filename, content, media_type,
                          delivery_mode, caption, reply_to_message_id):
    boundary = "ahc-" + uuid4().hex
    if boundary.encode() in content:
        raise ContractError("artifact multipart boundary collision")
    parts = []

    def part(name, data, *, media_type=None, upload_name=None):
        headers = f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"'
        if upload_name is not None:
            headers += f'; filename="{upload_name}"'
        if media_type is not None:
            headers += f"\r\nContent-Type: {media_type}"
        parts.append((headers + "\r\n\r\n").encode("utf-8") + data + b"\r\n")

    if route.platform == "telegram":
        part("chat_id", route.channel_id.encode())
        if delivery_mode == "document":
            part("disable_content_type_detection", b"true")
        if caption:
            part("caption", caption.encode("utf-8"))
        if reply_to_message_id is not None:
            part("reply_parameters", json.dumps(
                {"message_id": int(reply_to_message_id),
                 "allow_sending_without_reply": False},
                ensure_ascii=False,
            ).encode("utf-8"), media_type="application/json")
        part("photo" if delivery_mode == "photo" else "document", content,
             media_type=media_type, upload_name=filename)
    else:
        metadata = {"attachments": [{"id": 0, "filename": filename}],
                    "allowed_mentions": {"parse": []}}
        if caption:
            metadata["content"] = caption
        if reply_to_message_id is not None:
            metadata["message_reference"] = {
                "type": 0,
                "message_id": reply_to_message_id,
                "channel_id": route.channel_id,
                "fail_if_not_exists": True,
            }
            metadata["allowed_mentions"]["replied_user"] = False
        part("payload_json", json.dumps(metadata, ensure_ascii=False).encode("utf-8"),
             media_type="application/json")
        part("files[0]", content, media_type=media_type, upload_name=filename)
    body = b"".join(parts) + f"--{boundary}--\r\n".encode()
    return ArtifactUpload(route, artifact_id, filename, len(content),
                          f"multipart/form-data; boundary={boundary}", body,
                          delivery_mode, caption, reply_to_message_id)
