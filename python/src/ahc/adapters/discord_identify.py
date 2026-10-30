"""Fresh single-shard Identify preflight; construction performs no HTTP request."""

import http.client
import json
import time

from ahc.adapters.channel_text import _invalid_constant, _unique_object
from ahc.domain.sessions import ContractError


def _request(token):
    connection = http.client.HTTPSConnection("discord.com", timeout=10)
    try:
        connection.request("GET", "/api/v10/gateway/bot", headers={"Authorization": f"Bot {token}"})
        response = connection.getresponse()
        raw = response.read(65537)
        if response.status != 200 or len(raw) > 65536:
            raise ValueError()
        return json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    finally:
        connection.close()


class DiscordIdentifyAdmission:
    def __init__(self, token, *, request=_request, clock=time.monotonic):
        if (type(token) is not str or not 1 <= len(token) <= 512
                or any(not (c.isascii() and (c.isalnum() or c in ":._-")) for c in token)
                or not callable(request) or not callable(clock)):
            raise ContractError("Discord Identify admission binding invalid")
        self._token, self._request, self._clock = token, request, clock
        self._next = 0.0
        self.reason = "not-checked"

    def __call__(self):
        try:
            if self._clock() < self._next:
                self.reason = "identify-cooldown"
                return False
            document = self._request(self._token)
            if type(document) is not dict or type(document.get("shards")) is not int:
                raise ValueError()
            limits = document["session_start_limit"]
            if (type(limits) is not dict or any(type(limits.get(key)) is not int for key in
                    ("total", "remaining", "reset_after", "max_concurrency"))
                    or not 0 <= limits["remaining"] <= limits["total"]
                    or limits["total"] < 1 or limits["reset_after"] < 0
                    or limits["max_concurrency"] < 1):
                raise ValueError()
            if document["shards"] != 1:
                self.reason = "sharding-unsupported"
                return False
            if limits["remaining"] == 0:
                self.reason = "identify-quota-exhausted"
                return False
            # One single-shard driver. This isn't a reservation against other
            # applications sharing the bot; every Identify reads fresh limits.
            self._next = self._clock() + 5
            self.reason = "admitted"
            return True
        except Exception:
            self.reason = "identify-preflight-unavailable"
            return False
