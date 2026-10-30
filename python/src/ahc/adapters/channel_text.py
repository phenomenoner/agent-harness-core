"""One-shot bot text delivery over fixed platform endpoints."""

import http.client
import json
import re

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError

from .channel_formatting import discord_text, telegram_text


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("ambiguous channel response")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("invalid channel response number")


def _post(platform, token, channel, payload):
    host = "api.telegram.org" if platform == "telegram" else "discord.com"
    path = (f"/bot{token}/sendMessage" if platform == "telegram"
            else f"/api/v10/channels/{channel}/messages")
    headers = {"Content-Type": "application/json", "User-Agent": "AHC/0.3.0"}
    if platform == "discord":
        headers["Authorization"] = "Bot " + token
    connection = http.client.HTTPSConnection(host, timeout=10)
    try:
        connection.request("POST", path, json.dumps(payload).encode(), headers)
        response = connection.getresponse()
        data = response.read(65537)
        if response.status != 200 or len(data) > 65536:
            raise ValueError()
        return json.loads(data, object_pairs_hook=_unique_object,
                          parse_constant=_invalid_constant)
    finally:
        connection.close()


class BotTextTransport:
    """Trusted composition binds exact route/token; send cannot select another destination."""

    def __init__(self, route, token, *, post=_post, upload_post=None):
        if type(route) is not ChannelRoute or not callable(post):
            raise ContractError("channel transport binding invalid")
        if (type(token) is not str or not 1 <= len(token) <= 512
                or any(not (c.isascii() and (c.isalnum() or c in ":._-")) for c in token)):
            raise ContractError("channel token invalid")
        if not re.fullmatch(r"-?[0-9]{1,20}", route.channel_id):
            raise ContractError("channel destination requires a numeric ID")
        if route.platform == "discord" and not route.channel_id.isdigit():
            raise ContractError("Discord channel requires a positive ID")
        self._route, self._token, self._post = route, token, post
        if upload_post is not None and not callable(upload_post):
            raise ContractError("text document transport invalid")
        self._upload_post = upload_post

    def send(self, route, payload, logical_delivery_id):
        if route != self._route:
            raise ContractError("channel transport route mismatch")
        if (type(payload) is not dict or "output_text" not in payload
                or set(payload) - {"output_text", "reply_to_message_id"}):
            raise ContractError("channel text payload invalid")
        reply = payload.get("reply_to_message_id")
        if "reply_to_message_id" in payload:
            from ahc.domain.channel_reply import reply_message_id

            reply_message_id(reply)
        text = payload["output_text"]
        if (type(text) is not str or not text
                or any(0xD800 <= ord(c) <= 0xDFFF for c in text)):
            raise ContractError("channel text invalid")
        text_length = len(text.encode("utf-16-le")) // 2
        if text_length > 4096:
            from .channel_attachment import BotAttachmentTransport
            from ahc.application.channel_artifact_upload import prepare_text_upload

            upload = prepare_text_upload(route, text, reply_to_message_id=reply)
            options = {} if self._upload_post is None else {"post": self._upload_post}
            receipt = BotAttachmentTransport(route, self._token, **options).send(upload)
            return f"{route.platform}:{route.channel_id}:{receipt['message_id']}"
        try:
            body = ({"chat_id": route.channel_id, **telegram_text(text),
                     "link_preview_options": {"is_disabled": True}}
                    if route.platform == "telegram" else discord_text(text))
            if reply is not None:
                if route.platform == "telegram":
                    body["reply_parameters"] = {
                        "message_id": int(reply), "allow_sending_without_reply": False}
                else:
                    body["message_reference"] = {"type": 0, "message_id": reply,
                        "channel_id": route.channel_id, "fail_if_not_exists": True}
                    body["allowed_mentions"] = {"parse": [], "replied_user": False}
        except (TypeError, ValueError):
            raise ContractError("channel text exceeds platform limit") from None
        try:
            result = self._post(route.platform, self._token, route.channel_id, body)
            if route.platform == "telegram":
                if type(result) is not dict or result.get("ok") is not True:
                    raise ValueError()
                message = result["result"]
                message_id = message["message_id"]
                if type(message_id) is not int or message_id <= 0:
                    raise ValueError()
                channel, author = message["chat"]["id"], message["from"]["id"]
                if reply is not None:
                    reference = message["reply_to_message"]
                    if (type(reference["message_id"]) is not int
                            or str(reference["message_id"]) != reply
                            or type(reference["chat"]["id"]) is not int
                            or str(reference["chat"]["id"]) != route.channel_id):
                        raise ValueError()
            else:
                message_id = result["id"]
                if (type(message_id) is not str
                        or not re.fullmatch(r"[1-9][0-9]{0,19}", message_id)):
                    raise ValueError()
                channel, author = result["channel_id"], result["author"]["id"]
                if reply is not None:
                    reference = result["message_reference"]
                    if (reference["message_id"] != reply
                            or reference["channel_id"] != route.channel_id):
                        raise ValueError()
            if str(channel) != route.channel_id or str(author) != route.account_id:
                raise ValueError()
            return f"{route.platform}:{route.channel_id}:{message_id}"
        except Exception:
            raise ContractError("channel delivery response unverified") from None
