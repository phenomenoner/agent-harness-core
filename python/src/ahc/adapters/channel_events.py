"""Translate authenticated Telegram and Discord message events into local ingress."""

from __future__ import annotations

import re

from ahc.adapters.channel_file_refs import (
    ChannelFileDecodeError,
    decode_file_refs,
    describe_file_refs,
)
from ahc.adapters.channel_message_context import (
    ATTACHMENT_ONLY_PLACEHOLDER,
    MEDIA_ONLY_PLACEHOLDER,
    NOTICE_MAX_CHARS,
    TELEGRAM_UNSUPPORTED,
    bounded_id,
    context_for_message,
    discord_media,
    discord_reply,
    forward_fields,
    quote_fields,
    safe_attachment,
    telegram_media,
    telegram_reply,
)
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.turns import _input_text

_MEDIA_ONLY_NOTICE = "此訊息包含不支援的媒體，未執行。"
_DISABLED_FILES_NOTICE = "附件功能已停用，未執行此訊息。"
_INVALID_FILES_NOTICE = (
    "附件中繼資料無效或超過 1 MiB 上限，未執行此訊息。"
)
_MIXED_MEDIA_NOTICE = (
    "此訊息同時包含可支援檔案與不支援媒體，整則訊息未執行。"
)
_TEXT_TOO_LONG_NOTICE = (
    "訊息文字超過 4096 字元上限，未執行此訊息。"
)


def _snowflake(value):
    if type(value) is not str or not re.fullmatch(r"[1-9][0-9]{0,19}", value):
        raise ValueError()
    return value


def _integer(value, *, positive=True):
    if type(value) is not int or (positive and value <= 0):
        raise ValueError()
    return str(value)


def _invalid_event():
    raise ContractError("channel platform event invalid")


def _telegram_event(route, payload):
    if "message" not in payload:
        return None
    message = payload["message"]
    if type(message) is not dict:
        _invalid_event()
    try:
        author = message["from"]
        chat = message["chat"]
        if type(author) is not dict or type(chat) is not dict:
            raise ValueError()
        channel = _integer(chat["id"], positive=False)
        user = _integer(author["id"])
    except (KeyError, TypeError, ValueError):
        _invalid_event()
    if channel != route.channel_id or user != route.user_id:
        return None
    try:
        if author["is_bot"] is not False or "sender_chat" in message:
            return None
    except (KeyError, TypeError):
        _invalid_event()
    try:
        message_id = _integer(message["message_id"])
        event_id = "update-" + _integer(payload["update_id"], positive=False)
    except (KeyError, TypeError, ValueError):
        _invalid_event()
    return message, event_id, message_id


def _discord_event(route, payload):
    # Accept a Gateway MESSAGE_CREATE dispatch envelope, not arbitrary REST data.
    if payload.get("t") != "MESSAGE_CREATE":
        return None
    if type(payload.get("op")) is not int or payload["op"] != 0:
        _invalid_event()
    message = payload.get("d")
    if type(message) is not dict:
        _invalid_event()
    try:
        author = message["author"]
        if type(author) is not dict:
            raise ValueError()
        channel = _snowflake(message["channel_id"])
        user = _snowflake(author["id"])
    except (KeyError, TypeError, ValueError):
        _invalid_event()
    if channel != route.channel_id or user != route.user_id:
        return None
    if author.get("bot", False) is not False or "webhook_id" in message:
        return None
    if type(message.get("type")) is not int or message["type"] not in (0, 19):
        return None
    try:
        message_id = _snowflake(message["id"])
    except (KeyError, TypeError, ValueError):
        _invalid_event()
    return message, "message-" + message_id, message_id


def _message_text(message, platform):
    values = ("text", "caption") if platform == "telegram" else ("content",)
    for key in values:
        value = message.get(key)
        if value is not None and type(value) is not str:
            _invalid_event()
        if value:
            return value
    return None


def _telegram_unsupported_descriptions(message, status):
    result = []
    for key, (filename, media_type) in TELEGRAM_UNSUPPORTED.items():
        if key not in message:
            continue
        value = message[key]
        if type(value) is dict:
            filename = value.get("file_name", value.get("filename", filename))
            media_type = value.get("mime_type", value.get("content_type", media_type))
        result.append(safe_attachment(filename, media_type, status))
    return result


def _discord_descriptions(items, status):
    result = []
    for item in items[:8] if type(items) is list else [items]:
        if type(item) is dict:
            result.append(safe_attachment(item.get("filename", "attachment"),
                                          item.get("content_type", "application/octet-stream"),
                                          status))
        else:
            result.append(safe_attachment("attachment", "application/octet-stream", status))
    return result


def _media_descriptions(route, sources, status):
    descriptions = []
    for source in sources:
        if route.platform == "telegram":
            supported, _ = telegram_media(source)
            if supported:
                descriptions.extend(describe_file_refs(route, source, status=status))
            descriptions.extend(_telegram_unsupported_descriptions(source, status))
        else:
            supported, unsupported = discord_media(source)
            if supported:
                descriptions.extend(_discord_descriptions(supported, status))
            if unsupported:
                descriptions.extend(_discord_descriptions(unsupported, status))
    deduped = []
    seen = set()
    for item in descriptions:
        key = (item["filename"], item["media_type"], item["status"])
        if key not in seen and len(deduped) < 8:
            seen.add(key)
            deduped.append(item)
    return deduped


def _files_for_sources(route, sources):
    files = []
    seen = set()
    for source in sources:
        decoded = decode_file_refs(route, source)
        for item in decoded:
            if item.file_id not in seen:
                seen.add(item.file_id)
                files.append(item)
    if len(files) > 8:
        raise ChannelFileDecodeError("inbound file metadata exceeds the supported limit")
    return tuple(files)


def _notice_for_media(include_files, has_supported, has_unsupported, invalid_files):
    if invalid_files:
        return _INVALID_FILES_NOTICE
    if has_supported and has_unsupported:
        return _MIXED_MEDIA_NOTICE
    if has_unsupported:
        return _MEDIA_ONLY_NOTICE
    if has_supported and not include_files:
        return _DISABLED_FILES_NOTICE
    return None


def _decode_matching_message(route, message, event_id, message_id, *, include_files):
    current_text = _message_text(message, route.platform)
    if route.platform == "telegram":
        reference, reference_known = telegram_reply(route, message)
        supported, unsupported = telegram_media(message)
        reference_supported, reference_unsupported = telegram_media(reference or {})
    else:
        reference, reference_known, reference_header = discord_reply(route, message)
        supported, unsupported = discord_media(message)
        reference_supported, reference_unsupported = discord_media(reference or {})
    sources = [message]
    # A nested message is data only after its channel binding has been checked above.
    if reference is not None and (route.platform == "telegram" or reference_header is not None):
        sources.append(reference)

    has_supported = bool(supported or reference_supported)
    has_unsupported = bool(unsupported or reference_unsupported)
    invalid_files = False
    files = ()
    descriptors = []
    if has_supported or has_unsupported:
        descriptors = _media_descriptions(
            route, sources, "disabled" if not include_files else
            ("unsupported" if has_unsupported else "available"),
        )
    if include_files and has_supported and not has_unsupported:
        try:
            files = _files_for_sources(route, sources)
        except ChannelFileDecodeError:
            invalid_files = True
            files = ()
            descriptors = _media_descriptions(route, sources, "invalid")
    notice = _notice_for_media(include_files, has_supported, has_unsupported, invalid_files)
    if current_text is not None and len(current_text) > 4096:
        notice = _TEXT_TOO_LONG_NOTICE if notice is None else notice[:NOTICE_MAX_CHARS]
        files = ()
    if notice is not None:
        notice = notice[:NOTICE_MAX_CHARS]
        files = ()
    if files:
        descriptors = [
            safe_attachment(item.filename, item.media_type, "available") for item in files
        ]

    if current_text is not None and len(current_text) <= 4096:
        input_text = current_text
    elif files:
        input_text = ATTACHMENT_ONLY_PLACEHOLDER
    elif notice is not None:
        input_text = MEDIA_ONLY_PLACEHOLDER
    elif forward_fields(route, message, current_text).get("forwarded_text"):
        input_text = "Please examine the forwarded message."
    else:
        return None

    fields = quote_fields(
        route, message, reference, reference_known,
        reply_id=(reference_header.get("message_id")
                   if route.platform == "discord" and reference_header is not None else None),
    )
    fields.update(forward_fields(
        route, message, current_text if len(current_text or "") <= 4096 else None
    ))
    if route.platform == "telegram":
        media_group_id = bounded_id(message.get("media_group_id"))
        topic_id = bounded_id(message.get("message_thread_id"))
        if media_group_id is not None:
            fields["media_group_id"] = media_group_id
        if topic_id is not None:
            fields["topic_id"] = topic_id
    if descriptors:
        fields["attachments"] = descriptors[:8]
    if notice is not None:
        fields["notice"] = notice
    context = context_for_message(message_id, **fields)
    return ChannelInboundEvent(route, event_id, _input_text(input_text), files, context=context)


def decode_channel_event(route, payload, *, include_files=False):
    """Return one route-matching human event; provider authentication remains external."""

    if (type(route) is not ChannelRoute or type(payload) is not dict
            or type(include_files) is not bool):
        raise ContractError("channel event binding invalid")
    observed = (_telegram_event(route, payload) if route.platform == "telegram"
                else _discord_event(route, payload))
    if observed is None:
        return None
    message, event_id, message_id = observed
    return _decode_matching_message(route, message, event_id, message_id,
                                    include_files=include_files)


__all__ = ["decode_channel_event"]
