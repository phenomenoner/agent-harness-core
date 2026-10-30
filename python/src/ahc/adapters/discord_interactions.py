"""Native slash discovery and one-attempt interaction responses on the bot's Gateway."""

import http.client
import json
import math
import re
import time

from ahc.domain.channel_commands import discord_commands

from .channel_text import _invalid_constant, _unique_object


def _matches_command(actual, expected):
    def options(value):
        if type(value) is not list:
            return value
        return [{"required": False, **{k: v for k, v in row.items()
                 if k not in {"name_localizations", "description_localizations"} or v is not None}}
                if type(row) is dict else row for row in value]

    return all((options(actual.get(key, [])) == options(value) if key == "options"
                else actual.get(key) == value)
               for key, value in {"options": [], **expected}.items())


class DiscordRateLimited(ValueError):
    def __init__(self, retry_after):
        if (type(retry_after) not in (int, float) or not math.isfinite(retry_after)
                or retry_after < 0):
            raise ValueError("invalid Discord retry delay")
        self.retry_after = retry_after
        super().__init__("Discord request rejected by rate limit")


def _http(method, path, body, token=None, *, on_limit=None):
    headers = {"Content-Type": "application/json", "User-Agent": "AHC/0.3.0"}
    if token is not None:
        headers["Authorization"] = "Bot " + token
    connection = http.client.HTTPSConnection("discord.com", timeout=2)
    try:
        connection.request(
            method, "/api/v10" + path, None if body is None else json.dumps(body).encode(), headers
        )
        response = connection.getresponse()
        raw = response.read(131073)
        if len(raw) > 131072:
            raise ValueError("Discord interaction response too large")
        if response.status == 429:
            value = json.loads(raw)
            raise DiscordRateLimited(value.get("retry_after"))
        if response.status not in (200, 201, 204):
            raise ValueError("Discord interaction exchange unverified")
        if on_limit is not None and response.getheader("X-RateLimit-Remaining") == "0":
            delay = float(response.getheader("X-RateLimit-Reset-After", "0"))
            if not math.isfinite(delay) or delay < 0:
                raise ValueError("invalid Discord rate reset")
            on_limit(delay)
        return (
            True
            if response.status == 204
            else json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        )
    finally:
        connection.close()


class DiscordInteractions:
    def __init__(self, controls, routes, token, *, request=_http, clock=time.monotonic):
        self.controls, self.routes, self.token, self.request = (
            controls,
            tuple(routes),
            token,
            request,
        )
        self.menu_status = "not-attempted"
        self.clock, self._ready_after = clock, 0.0

    def _observe_limit(self, delay):
        self._ready_after = max(self._ready_after, self.clock() + delay)

    def configure(self, *, cancelled=lambda: False, wait=time.sleep):
        """One bounded startup pass; upsert owned names, preserve unrelated commands."""
        self.menu_status = "unverified"
        deadline, calls, pauses = self.clock() + 300, 0, 0

        def api(method, path, body=None):
            nonlocal calls, pauses
            for attempt in range(2):
                delay = max(0, self._ready_after - self.clock())
                if (cancelled() or self.clock() + delay >= deadline or calls >= 48
                        or pauses >= 8):
                    raise ValueError("command setup budget exhausted")
                if delay:
                    pauses += 1
                    wait(delay)
                if cancelled() or self.clock() >= deadline:
                    raise ValueError("command setup cancelled")
                calls += 1
                try:
                    if self.request is _http:
                        return _http(method, path, body, self.token, on_limit=self._observe_limit)
                    return self.request(method, path, body, self.token)
                except DiscordRateLimited as error:
                    self._observe_limit(error.retry_after)
                    if attempt:
                        raise

        try:
            account = self.routes[0].account_id
            me = api("GET", "/users/@me")
            app = api("GET", "/oauth2/applications/@me")
            if me.get("id") != account or me.get("bot") is not True or app.get("id") != account:
                return False
            scopes = set()
            for route in self.routes:
                if route.account_id != account or route.platform != "discord":
                    return False
                channel = api("GET", "/channels/" + route.channel_id)
                if channel.get("id") != route.channel_id:
                    return False
                guild = channel.get("guild_id")
                if guild is not None and not re.fullmatch(r"[1-9][0-9]{0,19}", guild):
                    return False
                if guild is None and channel.get("type") != 1:
                    return False
                scopes.add(guild)
            for guild in sorted(scopes, key=lambda value: value or ""):
                path = (
                    f"/applications/{account}" + (f"/guilds/{guild}" if guild else "") + "/commands"
                )
                prior = api("GET", path)
                if type(prior) is not list:
                    return False
                for entry in discord_commands():
                    if guild is None:
                        entry = {**entry, "contexts": [1], "integration_types": [0]}
                    old = next(
                        (r for r in prior if r.get("name") == entry["name"] and r.get("type") == 1),
                        None,
                    )
                    if old is not None and _matches_command(old, entry):
                        continue
                    result = api("POST", path, entry)
                    if (
                        result.get("name") != entry["name"]
                        or result.get("application_id") != account
                    ):
                        return False
            self.menu_status = "configured"
            return True
        except Exception:
            return False

    def respond(self, frame, receipts):
        if frame.get("t") != "INTERACTION_CREATE":
            return
        interaction = frame.get("d", {})
        identifier, token = interaction.get("id"), interaction.get("token")
        if (
            type(identifier) is not str
            or not re.fullmatch(r"[1-9][0-9]{0,19}", identifier)
            or type(token) is not str
            or not re.fullmatch(r"[A-Za-z0-9._-]{1,2048}", token)
        ):
            # Claimed rows remain sending until startup reconciliation marks unknown.
            return
        for item in receipts:
            response = item.get("interaction_response")
            if response is None:
                continue
            state, receipt = "unknown", None
            text = response["reply"]
            content = (
                {"content": text}
                if len(text) <= 2000
                else {"embeds": [{"description": text[:4096]}]}
            )
            try:
                result = self.request(
                    "POST",
                    f"/interactions/{identifier}/{token}/callback",
                    {
                        "type": 4,
                        "data": {**content, "flags": 64, "allowed_mentions": {"parse": []}},
                    },
                )
                if result is not True:
                    raise ValueError()
                state, receipt = "sent", "discord-interaction:" + identifier
            except Exception:
                pass  # Never retry an uncertain interaction response.
            operation = response.get("operation_id")
            if operation is not None:
                self.controls.store._mutate(
                    lambda c, state=state, receipt=receipt, operation=operation: c.execute(
                        "UPDATE channel_controls SET state=?,receipt=? "
                        "WHERE operation_id=? AND state='sending'",
                        (state, receipt, operation),
                    )
                )
