"""One ephemeral typing request; scheduling and turn authority belong to the caller."""

import http.client
import json

from ahc.adapters.channel_text import BotTextTransport, _invalid_constant, _unique_object
from ahc.domain.sessions import ContractError


def _post(route, token):
    telegram = route.platform == "telegram"
    host = "api.telegram.org" if telegram else "discord.com"
    path = (f"/bot{token}/sendChatAction" if telegram else
            f"/api/v10/channels/{route.channel_id}/typing")
    headers = {"Content-Type": "application/json", "User-Agent": "AHC/0.3.0"}
    if not telegram:
        headers["Authorization"] = "Bot " + token
    body = (json.dumps({"chat_id": route.channel_id, "action": "typing"}).encode()
            if telegram else b"")
    connection = http.client.HTTPSConnection(host, timeout=5)
    try:
        connection.request("POST", path, body, headers)
        response = connection.getresponse()
        data = response.read(4097)
        if len(data) > 4096:
            raise ValueError()
        if not telegram:
            return response.status == 204 and data == b""
        if response.status != 200:
            return False
        value = json.loads(data, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        return type(value) is dict and value.get("ok") is True and value.get("result") is True
    finally:
        connection.close()


class BotTypingTransport:
    def __init__(self, route, token, *, post=_post):
        BotTextTransport(route, token)
        if not callable(post):
            raise ContractError("typing transport invalid")
        self._route, self._token, self._post = route, token, post

    def send(self):
        try:
            if self._post(self._route, self._token) is not True:
                raise ValueError()
            return {"status": "acknowledged", "kind": "typing"}
        except Exception:
            raise ContractError("typing request unverified") from None
