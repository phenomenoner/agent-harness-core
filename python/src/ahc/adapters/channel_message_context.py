"""Small, provider-neutral helpers for bounded inbound message context."""

from __future__ import annotations

from ahc.domain.skill_values import canonical_json

CONTEXT_MAX_CHARS = 8000
QUOTE_MAX_CHARS = 3000
FORWARDED_TEXT_MAX_CHARS = 3000
NOTICE_MAX_CHARS = 500

MEDIA_ONLY_PLACEHOLDER = "I received a message that cannot be executed."
ATTACHMENT_ONLY_PLACEHOLDER = "Please examine the attached files."

TELEGRAM_SUPPORTED = frozenset({"document", "photo"})
TELEGRAM_UNSUPPORTED = {
    "voice": ("voice", "audio/ogg"),
    "audio": ("audio", "audio/octet-stream"),
    "video": ("video", "video/octet-stream"),
    "animation": ("animation", "video/octet-stream"),
    "video_note": ("video_note", "video/octet-stream"),
    "sticker": ("sticker", "image/webp"),
    "location": ("location", "application/x-ahc-location"),
    "venue": ("venue", "application/x-ahc-venue"),
    "contact": ("contact", "application/x-ahc-contact"),
    "poll": ("poll", "application/x-ahc-poll"),
}


def bounded_id(value: object) -> str | None:
    """Return a small opaque identifier without preserving provider payloads."""

    if isinstance(value, bool):
        return None
    if type(value) is int:
        value = str(value)
    if type(value) is not str or not value or len(value) > 128:
        return None
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        return None
    return value


def bounded_text(value: object, maximum: int) -> str | None:
    if type(value) is not str or not value:
        return None
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        return None
    return value[:maximum]


def quote_value(value: object) -> tuple[str | None, str | None]:
    """Bound quote text and expose whether the provider text was truncated."""

    value = bounded_text(value, QUOTE_MAX_CHARS + 1)
    if value is None:
        return None, None
    if len(value) > QUOTE_MAX_CHARS:
        return value[:QUOTE_MAX_CHARS], "truncated"
    return value, "available"


def safe_attachment(filename: object, media_type: object, status: str) -> dict[str, str]:
    """Build an attachment projection without IDs, URLs, or raw provider data."""

    filename = bounded_text(filename, 128) or "attachment"
    media_type = bounded_text(media_type, 128) or "application/octet-stream"
    if "/" not in media_type or any(ord(character) < 0x20 for character in media_type):
        media_type = "application/octet-stream"
    status = bounded_text(status, 128) or "unknown"
    return {"filename": filename, "media_type": media_type, "status": status}


def telegram_reply(route, message):
    # None marks unusable optional context; False marks absent/foreign context.
    has_reference = any(key in message for key in ("reply_to_message", "reply_to_message_id"))
    reference = message.get("reply_to_message")
    if reference is None:
        return None, has_reference
    if type(reference) is not dict:
        return None, None
    chat = reference.get("chat")
    if chat is not None:
        if type(chat) is not dict or "id" not in chat:
            return None, None
        chat_id = chat["id"]
        if isinstance(chat_id, bool) or type(chat_id) not in (int, str):
            return None, None
        if str(chat_id) != route.channel_id:
            return None, False
    return reference, True


def discord_reply(route, message):
    # Do not inspect quoted content/files until its reference is usable and bound.
    reference = message.get("message_reference")
    if reference is None or type(reference) is not dict:
        if reference is not None:
            return None, None, None
        return None, False, None
    channel = reference.get("channel_id")
    if channel is not None and type(channel) is not str:
        return None, None, None
    if channel is not None and channel != route.channel_id:
        return None, False, None
    reply_id = bounded_id(reference.get("message_id"))
    if reply_id is None:
        return None, None, None
    referenced = message.get("referenced_message")
    if referenced is None:
        return None, True, reference
    if type(referenced) is not dict:
        return None, None, None
    referenced_channel = referenced.get("channel_id")
    if referenced_channel is not None and type(referenced_channel) is not str:
        return None, None, None
    if referenced_channel is not None and referenced_channel != route.channel_id:
        return None, False, None
    referenced_id = bounded_id(referenced.get("id"))
    if "id" in referenced and referenced_id is None:
        return None, None, None
    if referenced_id is not None and referenced_id != reference.get("message_id"):
        return None, False, None
    return referenced, True, reference


def quote_fields(route, message, reference, reference_known, *, reply_id=None):
    if reference_known is None:
        return {"quote_status": "unavailable"}
    if route.platform == "telegram":
        if not reference_known and any(
            key in message for key in ("reply_to_message", "reply_to_message_id")
        ):
            return {}
        quote_object = message.get("quote")
        candidate = quote_object.get("text") if type(quote_object) is dict else None
        if not candidate and reference is not None:
            candidate = reference.get("text") or reference.get("caption")
        reply_id = bounded_id(reference.get("message_id")) if reference is not None else None
        if reply_id is None:
            reply_id = bounded_id(message.get("reply_to_message_id"))
        fields = {}
        if reply_id is not None:
            fields["reply_to_id"] = reply_id
        quote, status = quote_value(candidate)
        if quote is not None:
            fields["quote"] = quote
            fields["quote_status"] = status
        elif reference_known:
            fields["quote_status"] = "unavailable"
        return fields

    if not reference_known:
        return {}
    reply_id = bounded_id(reply_id)
    if reply_id is None and reference is not None:
        reply_id = bounded_id(reference.get("message_id"))
    if reply_id is None:
        return {"quote_status": "unavailable"}
    fields = {"reply_to_id": reply_id}
    candidate = reference.get("content") if reference is not None else None
    quote, status = quote_value(candidate)
    if quote is None:
        fields["quote_status"] = "unavailable"
    else:
        fields["quote"] = quote
        fields["quote_status"] = status
    return fields


def forward_fields(route, message, current_text):
    marker = False
    forwarded_text = None
    if route.platform == "telegram":
        marker = any(key in message for key in (
            "forward_origin", "forward_from", "forward_from_chat", "forward_sender_name",
        ))
        forwarded_text = current_text if marker else None
    else:
        snapshots = message.get("message_snapshots")
        marker = type(snapshots) is list and bool(snapshots)
        if marker:
            for snapshot in snapshots:
                nested = snapshot.get("message", snapshot) if type(snapshot) is dict else {}
                if type(nested) is dict and type(nested.get("content")) is str:
                    forwarded_text = nested["content"]
                    break
            forwarded_text = forwarded_text or current_text
    if not marker:
        return {}
    fields = {"forwarded": True}
    forwarded_text = bounded_text(forwarded_text, FORWARDED_TEXT_MAX_CHARS)
    if forwarded_text is not None:
        fields["forwarded_text"] = forwarded_text
    return fields


def telegram_media(message):
    supported = [key for key in TELEGRAM_SUPPORTED if key in message]
    unsupported = [key for key in TELEGRAM_UNSUPPORTED if key in message]
    document = message.get("document")
    mime = document.get("mime_type", "") if type(document) is dict else ""
    if type(mime) is str and mime.startswith(("audio/", "video/")):
        supported.remove("document")
        unsupported.append("document")
    return supported, unsupported


def discord_media(message):
    supported = []
    unsupported = []
    if "attachments" in message:
        attachments = message.get("attachments")
        if type(attachments) is list:
            for item in attachments:
                media_type = item.get("content_type") if type(item) is dict else None
                media_type = media_type.lower() if type(media_type) is str else ""
                target = unsupported if media_type.startswith(("audio/", "video/")) else supported
                target.append(item)
        else:
            supported.append(attachments)
    for key in TELEGRAM_UNSUPPORTED:
        if key in message:
            unsupported.append(key)
    if message.get("sticker_items"):
        unsupported.append("sticker_items")
    snapshots = message.get("message_snapshots", [])
    if type(snapshots) is list and any(
            type(s) is dict and type(s.get("message")) is dict
            and (s["message"].get("attachments") or s["message"].get("sticker_items"))
            for s in snapshots):
        unsupported.append("forwarded_media")
    return supported, unsupported


def finalize_context(context: dict[str, object]) -> dict[str, object] | None:
    """Omit empty values and keep the canonical provider context bounded."""

    result: dict[str, object] = {}
    for key, value in context.items():
        if value is None or value == "" or value == []:
            continue
        if key == "attachments":
            value = list(value[:8]) if isinstance(value, list) else []
            if not value:
                continue
        result[key] = value

    encoded = canonical_json(result).decode()
    if len(encoded) <= CONTEXT_MAX_CHARS:
        return result or None

    # Keep identity and the deterministic notice before optional quoted/forwarded data.
    for key, limit in (("forwarded_text", 1000), ("quote", 1000)):
        value = result.get(key)
        if type(value) is str and len(value) > limit:
            result[key] = value[:limit]
            if key == "quote":
                result["quote_status"] = "truncated"
    encoded = canonical_json(result).decode()
    if len(encoded) <= CONTEXT_MAX_CHARS:
        return result

    result.pop("attachments", None)
    encoded = canonical_json(result).decode()
    if len(encoded) <= CONTEXT_MAX_CHARS:
        return result

    # Keep bounded identity and notice if future extensions still exceed the limit.
    return {key: result[key] for key in ("message_id", "notice") if key in result} or None


def context_for_message(message_id: object, **fields: object) -> dict[str, object] | None:
    context: dict[str, object] = {}
    message_id = bounded_id(message_id)
    if message_id is not None:
        context["message_id"] = message_id
    context.update(fields)
    return finalize_context(context)


__all__ = [
    "ATTACHMENT_ONLY_PLACEHOLDER",
    "CONTEXT_MAX_CHARS",
    "FORWARDED_TEXT_MAX_CHARS",
    "MEDIA_ONLY_PLACEHOLDER",
    "NOTICE_MAX_CHARS",
    "TELEGRAM_SUPPORTED",
    "TELEGRAM_UNSUPPORTED",
    "QUOTE_MAX_CHARS",
    "bounded_id",
    "bounded_text",
    "context_for_message",
    "finalize_context",
    "quote_value",
    "safe_attachment",
    "discord_media",
    "discord_reply",
    "forward_fields",
    "quote_fields",
    "telegram_media",
    "telegram_reply",
]
