"""Decode bounded file references only after route and message authentication."""

from __future__ import annotations

from ahc.adapters.channel_message_context import safe_attachment
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError

MAX_FILE_BYTES = 1_048_576


class ChannelFileDecodeError(ContractError):
    """Authenticated message file metadata could not be admitted."""


def _channel_file(route, file_id, filename, byte_count, media_type, download_url=None):
    return ChannelFile(route.platform, route.channel_id, file_id, filename, byte_count,
                       media_type, download_url)


def _decode_telegram_document(route, document):
    if type(document) is not dict:
        raise ValueError()
    return _channel_file(
        route,
        document["file_id"],
        document.get("file_name", "attachment.bin"),
        document["file_size"],
        document.get("mime_type", "application/octet-stream"),
    )


def _decode_telegram_photo(route, message, photo):
    if type(photo) is not list or not photo:
        raise ValueError()
    candidates = []
    for ordinal, item in enumerate(photo):
        if type(item) is not dict:
            raise ValueError()
        size = item["file_size"]
        if type(size) is not int or isinstance(size, bool) or size < 0:
            raise ValueError()
        # Oversized photo sizes are skipped so a smaller provider rendition can be used.
        if size <= MAX_FILE_BYTES:
            width = item.get("width", 0)
            height = item.get("height", 0)
            if (type(width) is not int or isinstance(width, bool) or width < 0
                    or type(height) is not int or isinstance(height, bool) or height < 0):
                raise ValueError()
            candidates.append((size, width, height, ordinal, item))
    if not candidates:
        raise ValueError()
    _, _, _, _, selected = max(candidates, key=lambda value: value[:4])
    message_id = message["message_id"]
    filename = f"photo-{message_id}.jpg"
    return _channel_file(route, selected["file_id"], filename,
                         selected["file_size"], "image/jpeg")


def _decode_telegram(route, message):
    if "document" in message and "photo" in message:
        raise ValueError()
    if "document" in message:
        return (_decode_telegram_document(route, message["document"]),)
    if "photo" in message:
        return (_decode_telegram_photo(route, message, message["photo"]),)
    return ()


def _decode_discord(route, message):
    attachments = message.get("attachments", [])
    if type(attachments) is not list or len(attachments) > 8:
        raise ValueError()
    result = tuple(
        _channel_file(
            route,
            item["id"],
            item["filename"],
            item["size"],
            item.get("content_type") or "application/octet-stream",
            item["url"],
        )
        for item in attachments
    )
    if len({item.file_id for item in result}) != len(result):
        raise ValueError()
    return result


def decode_file_refs(route, message):
    """Return validated references, or a typed metadata error after route matching."""

    if type(route) is not ChannelRoute or type(message) is not dict:
        raise ContractError("inbound file binding invalid")
    try:
        result = _decode_telegram(route, message) if route.platform == "telegram" else (
            _decode_discord(route, message)
        )
        return result
    except (KeyError, TypeError, ValueError, ContractError):
        raise ChannelFileDecodeError("inbound file metadata invalid") from None


def describe_file_refs(route, message, *, status):
    """Project filenames and media types for context without IDs or download URLs."""

    if type(route) is not ChannelRoute or type(message) is not dict:
        raise ContractError("inbound file binding invalid")
    if route.platform == "telegram":
        if "document" in message:
            document = message.get("document")
            if type(document) is not dict:
                return [safe_attachment("attachment", "application/octet-stream", status)]
            return [safe_attachment(document.get("file_name", "attachment.bin"),
                                    document.get("mime_type", "application/octet-stream"),
                                    status)]
        if "photo" in message:
            message_id = message.get("message_id", "unknown")
            return [safe_attachment(f"photo-{message_id}.jpg", "image/jpeg", status)]
        return []
    attachments = message.get("attachments", [])
    if type(attachments) is not list:
        return [safe_attachment("attachment", "application/octet-stream", status)]
    return [
        safe_attachment(
            item.get("filename", "attachment") if type(item) is dict else "attachment",
            item.get("content_type", "application/octet-stream")
            if type(item) is dict else "application/octet-stream",
            status,
        )
        for item in attachments[:8]
    ]


__all__ = [
    "ChannelFileDecodeError",
    "MAX_FILE_BYTES",
    "decode_file_refs",
    "describe_file_refs",
]
