"""Edit only the exact bot message acknowledged for this turn's activity card."""

import http.client
import json
import re

from ahc.domain.sessions import ContractError

from .channel_formatting import discord_text, telegram_text
from .channel_text import BotTextTransport, _invalid_constant, _unique_object


def _edit(platform, token, channel, message, text):
    telegram = platform == "telegram"
    path = (f"/bot{token}/editMessageText" if telegram else
            f"/api/v10/channels/{channel}/messages/{message}")
    try:
        body = ({"chat_id": channel, "message_id": int(message), **telegram_text(text),
                 "link_preview_options": {"is_disabled": True}} if telegram else
                discord_text(text))
    except (TypeError, ValueError):
        raise ContractError("activity text exceeds platform limit") from None
    headers = {"Content-Type": "application/json", "User-Agent": "AHC/0.3.0"}
    if not telegram:
        headers["Authorization"] = "Bot " + token
    connection = http.client.HTTPSConnection(
        "api.telegram.org" if telegram else "discord.com", timeout=5)
    try:
        connection.request("POST" if telegram else "PATCH", path,
                           json.dumps(body).encode(), headers)
        response = connection.getresponse()
        data = response.read(65537)
        if response.status != 200 or len(data) > 65536:
            raise ValueError()
        return json.loads(data, object_pairs_hook=_unique_object,
                          parse_constant=_invalid_constant)
    finally:
        connection.close()


class BotActivityTransport:
    def __init__(self, route, token, *, text_transport=None, edit=_edit):
        self.text = text_transport or BotTextTransport(route, token)
        self.route, self.token, self._edit = route, token, edit
        self.receipt = None
        self.attempted = False

    def create(self, text, operation_id):
        if self.attempted:
            raise ContractError("activity create already attempted")
        self.attempted = True
        receipt = self.text.send(self.route, {"output_text": text}, operation_id)
        prefix = f"{self.route.platform}:{self.route.channel_id}:"
        if (not receipt.startswith(prefix)
                or not re.fullmatch(r"[1-9][0-9]{0,19}", receipt[len(prefix):])):
            raise ContractError("activity receipt invalid")
        self.receipt = receipt
        return receipt

    def update(self, text):
        if (self.receipt is None or type(text) is not str or not 1 <= len(text) <= 1500
                or any(0xD800 <= ord(c) <= 0xDFFF for c in text)):
            raise ContractError("activity update invalid")
        route = self.route
        message_id = self.receipt.rsplit(":", 1)[1]
        try:
            result = self._edit(route.platform, self.token, route.channel_id, message_id, text)
            if route.platform == "telegram":
                if result.get("ok") is not True:
                    raise ValueError()
                message = result["result"]
                identity = (str(message["message_id"]), str(message["chat"]["id"]),
                            str(message["from"]["id"]))
            else:
                identity = (result["id"], result["channel_id"], result["author"]["id"])
            if identity != (message_id, route.channel_id, route.account_id):
                raise ValueError()
        except Exception:
            raise ContractError("activity update unverified") from None
