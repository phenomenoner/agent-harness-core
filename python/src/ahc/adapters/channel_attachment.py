"""One-shot multipart attachment transport; durable delivery policy belongs to its caller."""

import http.client
import json
import re

from ahc.adapters.channel_text import BotTextTransport, _invalid_constant, _unique_object
from ahc.application.channel_artifact_upload import ArtifactUpload
from ahc.domain.channel_photo import MAX_PHOTO_DIMENSION, MAX_PHOTO_RATIO
from ahc.domain.channel_reply import reply_message_id
from ahc.domain.result_artifacts import validate_caption, validate_delivery_mode
from ahc.domain.sessions import ContractError

_MAX_TELEGRAM_PHOTO_SIZES = 10
_MAX_TELEGRAM_PHOTO_FILE_SIZE = 10 * 1_048_576
_FILE_ID = r"[A-Za-z0-9_-]{1,512}"
_SNOWFLAKE = r"[1-9][0-9]{0,19}"


def _post(route, token, upload):
    host = "api.telegram.org" if route.platform == "telegram" else "discord.com"
    if route.platform == "telegram":
        path = (f"/bot{token}/sendPhoto" if upload.delivery_mode == "photo"
                else f"/bot{token}/sendDocument")
    else:
        path = f"/api/v10/channels/{route.channel_id}/messages"
    headers = {"Content-Type": upload.content_type}
    if route.platform == "discord":
        headers["Authorization"] = "Bot " + token
    connection = http.client.HTTPSConnection(host, timeout=10)
    try:
        connection.request("POST", path, upload.body, headers)
        response = connection.getresponse()
        data = response.read(65537)
        if response.status != 200 or len(data) > 65536:
            raise ValueError()
        return json.loads(data, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    finally:
        connection.close()


class BotAttachmentTransport:
    def __init__(self, route, token, *, post=_post):
        BotTextTransport(route, token)
        if not callable(post):
            raise ContractError("attachment transport invalid")
        self._route, self._token, self._post = route, token, post

    def send(self, upload):
        if (type(upload) is not ArtifactUpload or upload.route != self._route
                or type(upload.body) is not bytes
                or type(upload.byte_count) is not int or upload.byte_count < 0
                or len(upload.body) <= upload.byte_count
                or type(upload.content_type) is not str
                or not re.fullmatch(r"multipart/form-data; boundary=ahc-[0-9a-f]{32}",
                                    upload.content_type)):
            raise ContractError("attachment transport binding invalid")
        try:
            validate_delivery_mode(upload.delivery_mode)
            validate_caption(upload.caption)
            if upload.reply_to_message_id is not None:
                reply_message_id(upload.reply_to_message_id)
        except ContractError:
            raise ContractError("attachment transport binding invalid") from None
        route = self._route
        try:
            result = self._post(route, self._token, upload)
            if route.platform == "telegram":
                if result["ok"] is not True:
                    raise ValueError()
                message = result["result"]
                message_id = message["message_id"]
                if type(message_id) is not int or message_id <= 0:
                    raise ValueError()
                channel, author = message["chat"]["id"], message["from"]["id"]
                if type(channel) is not int or type(author) is not int:
                    raise ValueError()
                if upload.reply_to_message_id is not None:
                    reference = message["reply_to_message"]
                    if type(reference) is not dict:
                        raise ValueError()
                    referenced_id = reference["message_id"]
                    referenced_chat = reference["chat"]["id"]
                    if (type(referenced_id) is not int or referenced_id <= 0
                            or str(referenced_id) != upload.reply_to_message_id
                            or type(referenced_chat) is not int
                            or str(referenced_chat) != route.channel_id):
                        raise ValueError()
                if upload.delivery_mode == "photo":
                    photos = message["photo"]
                    if (type(photos) is not list
                            or not 0 < len(photos) <= _MAX_TELEGRAM_PHOTO_SIZES):
                        raise ValueError()
                    validated = []
                    for photo in photos:
                        if type(photo) is not dict:
                            raise ValueError()
                        file_id = photo["file_id"]
                        width, height, file_size = (
                            photo["width"], photo["height"], photo["file_size"]
                        )
                        if (type(file_id) is not str or not re.fullmatch(_FILE_ID, file_id)
                                or type(width) is not int or isinstance(width, bool)
                                or not 0 < width <= MAX_PHOTO_DIMENSION
                                or type(height) is not int or isinstance(height, bool)
                                or not 0 < height <= MAX_PHOTO_DIMENSION
                                or width + height > 10_000
                                or max(width, height) > MAX_PHOTO_RATIO * min(width, height)
                                or type(file_size) is not int or isinstance(file_size, bool)
                                or not 0 < file_size <= _MAX_TELEGRAM_PHOTO_FILE_SIZE):
                            raise ValueError()
                        validated.append((file_size, width, height, file_id))
                    attachment_id = max(validated)[3]
                else:
                    attachment = message["document"]
                    attachment_id = attachment["file_id"]
                    filename, size = attachment["file_name"], attachment["file_size"]
            else:
                message_id = result["id"]
                if type(message_id) is not str or not re.fullmatch(r"[1-9][0-9]{0,19}", message_id):
                    raise ValueError()
                channel, author = result["channel_id"], result["author"]["id"]
                if type(channel) is not str or type(author) is not str:
                    raise ValueError()
                if upload.reply_to_message_id is not None:
                    reference = result["message_reference"]
                    if (type(reference) is not dict
                            or reference.get("message_id") != upload.reply_to_message_id
                            or reference.get("channel_id") != route.channel_id):
                        raise ValueError()
                if type(result["attachments"]) is not list or len(result["attachments"]) != 1:
                    raise ValueError()
                attachment = result["attachments"][0]
                attachment_id = attachment["id"]
                if (type(attachment_id) is not str
                        or not re.fullmatch(_SNOWFLAKE, attachment_id)):
                    raise ValueError()
                filename, size = attachment["filename"], attachment["size"]
            if (str(channel) != route.channel_id or str(author) != route.account_id
                    or type(attachment_id) is not str
                    or not re.fullmatch(_FILE_ID, attachment_id)):
                raise ValueError()
            if upload.delivery_mode == "photo":
                if route.platform == "discord":
                    if (filename != upload.filename or type(size) is not int
                            or isinstance(size, bool) or size != upload.byte_count):
                        raise ValueError()
                    receipt = {
                        "artifact_id": upload.artifact_id,
                        "message_id": str(message_id),
                        "attachment_id": attachment_id,
                        "filename": filename,
                        "byte_count": size,
                    }
                else:
                    receipt = {
                        "artifact_id": upload.artifact_id,
                        "delivery_mode": "photo",
                        "source_byte_count": upload.byte_count,
                        "message_id": str(message_id),
                        "attachment_id": attachment_id,
                    }
                receipt["delivery_mode"] = "photo"
                receipt.setdefault("source_byte_count", upload.byte_count)
                return receipt
            if (filename != upload.filename or type(size) is not int
                    or isinstance(size, bool) or size != upload.byte_count):
                raise ValueError()
            return {"artifact_id": upload.artifact_id, "message_id": str(message_id),
                    "attachment_id": attachment_id, "filename": filename, "byte_count": size}
        except Exception:
            raise ContractError("attachment delivery response unverified") from None
