"""Bounded Telegram polling; only a durable consumer may supply the next offset."""

import http.client
import json
import re

from ahc.adapters.channel_text import _invalid_constant, _unique_object
from ahc.domain.sessions import ContractError


class TelegramPollingUnavailable(ContractError):
    """The platform exchange is unverified; no new batch is available for admission."""

    def __init__(self, message, *, retryable=False, category=None, http_status=None,
                 retry_after=None):
        super().__init__(message)
        self.retryable = retryable is True
        allowed = {"transport", "unverified-response", "service", "rate-limit",
                   "authorization", "conflict"}
        self.category = (category if type(category) is str and category in allowed else
                         "transport" if self.retryable else "unverified-response")
        self.http_status = (http_status if type(http_status) is int
                            and 100 <= http_status <= 599 else None)
        self.retry_after = (retry_after if type(retry_after) is int
                            and 1 <= retry_after <= 86400 else None)


def _request(token, method, body):
    if method not in {"getMe", "getUpdates", "answerCallbackQuery", "getMyCommands",
                      "setMyCommands", "setChatMenuButton"}:
        raise ContractError("Telegram polling method unsupported")
    connection = http.client.HTTPSConnection("api.telegram.org", timeout=10)
    try:
        connection.request("POST", f"/bot{token}/{method}", json.dumps(body).encode(),
                           {"Content-Type": "application/json"})
        response = connection.getresponse()
        raw = response.read(1048577)
        if len(raw) > 1048576:
            raise ValueError()
        if response.status != 200:
            category = ("rate-limit" if response.status == 429 else
                        "service" if 500 <= response.status <= 599 else
                        "authorization" if response.status in {401, 403} else
                        "conflict" if response.status == 409 else "unverified-response")
            retry_after = None
            if response.status == 429:
                try:
                    failure = json.loads(raw, object_pairs_hook=_unique_object,
                                         parse_constant=_invalid_constant)
                    retry_after = failure.get("parameters", {}).get("retry_after")
                except (ValueError, AttributeError):
                    pass
            raise TelegramPollingUnavailable("Telegram polling response unverified",
                retryable=category in {"service", "rate-limit"}, category=category,
                http_status=response.status, retry_after=retry_after)
        return json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    finally:
        connection.close()


class TelegramPoll:
    """One explicitly bound bot; no webhook deletion, offset guessing or internal retries."""

    def __init__(self, account_id, token, *, request=_request, interactions=False):
        if (type(account_id) is not str or not 1 <= len(account_id) <= 19
                or not account_id.isascii()
                or not account_id.isdigit() or not 0 < int(account_id) < 2**63
                or not callable(request)):
            raise ContractError("Telegram polling account invalid")
        self.account_id = account_id
        if type(interactions) is not bool:
            raise ContractError("Telegram interactions option invalid")
        self.interactions = interactions
        self._token, self._request = token, request
        # Token validation is independent of route and must happen before getMe.
        if (type(token) is not str or not 1 <= len(token) <= 512
                or any(not (c.isascii() and (c.isalnum() or c in ":._-")) for c in token)):
            raise ContractError("Telegram polling token invalid")

    def _identity(self):
        identity = self._request(self._token, "getMe", {})
        bot = identity["result"]
        if (identity["ok"] is not True or bot["is_bot"] is not True
                or type(bot["id"]) is not int or str(bot["id"]) != self.account_id):
            raise ValueError()
        return bot

    def probe(self):
        """Read-only bound-bot health check; never consumes or acknowledges updates."""
        try:
            self._identity()
        except TelegramPollingUnavailable:
            raise
        except (OSError, http.client.HTTPException):
            raise TelegramPollingUnavailable(
                "Telegram polling response unverified", retryable=True) from None
        except Exception:
            raise TelegramPollingUnavailable("Telegram polling response unverified") from None

    def poll(self, offset=None):
        if offset is not None and (type(offset) is not int or not 0 <= offset < 2**63):
            raise ContractError("Telegram polling offset invalid")
        try:
            bot = self._identity()
            body = {"limit": 16, "timeout": 0, "allowed_updates": (
                ["message", "callback_query"] if self.interactions else ["message"])}
            if offset is not None:
                body["offset"] = offset
            response = self._request(self._token, "getUpdates", body)
            rows = response["result"]
            if response["ok"] is not True or type(rows) is not list or len(rows) > 16:
                raise ValueError()
            previous = -1 if offset is None else offset - 1
            for row in rows:
                value = row["update_id"]
                if type(value) is not int or not previous < value < 2**63 - 1:
                    raise ValueError()
                previous = value
                message = row.get("message")
                if type(message) is dict and type(message.get("text")) is str:
                    match = re.match(r"^(/[a-zA-Z][a-zA-Z0-9_]*)@([A-Za-z0-9_]+)(?=\s|$)",
                                     message["text"])
                    username = bot.get("username")
                    if match and type(username) is str:
                        if match[2].lower() == username.lower():
                            message["text"] = match[1] + message["text"][match.end():]
                        else:
                            message.pop("text")  # A command addressed to another bot.
            return rows
        except TelegramPollingUnavailable:
            raise
        except (OSError, http.client.HTTPException):
            raise TelegramPollingUnavailable(
                "Telegram polling response unverified", retryable=True) from None
        except Exception:
            raise TelegramPollingUnavailable("Telegram polling response unverified") from None

    def answer_callback(self, callback):
        """One optional toast attempt after the inbox transaction has committed."""
        try:
            result = self._request(self._token, "answerCallbackQuery", {
                "callback_query_id": callback["id"], "text": callback["text"], "cache_time": 0})
            return result.get("ok") is True and result.get("result") is True
        except Exception:
            return False  # No retry and no replay of the associated decision.

    def configure_commands(self, routes):
        """One startup reconciliation, scoped to configured chats/members only."""
        from ahc.domain.channel_commands import COMMANDS

        commands = [{"command": name, "description": description}
                    for name, description, _ in COMMANDS]
        try:
            for route in routes:
                if route.account_id != self.account_id or route.platform != "telegram":
                    raise ContractError("Telegram command menu route mismatch")
                scope = ({"type": "chat", "chat_id": route.channel_id}
                         if not route.channel_id.startswith("-") else
                         {"type": "chat_member", "chat_id": route.channel_id,
                          "user_id": int(route.user_id)})
                prior = self._request(self._token, "getMyCommands", {"scope": scope})
                if prior.get("ok") is not True or type(prior.get("result")) is not list:
                    return False
                if prior["result"] != commands:
                    result = self._request(self._token, "setMyCommands",
                                           {"scope": scope, "commands": commands})
                    if result.get("ok") is not True or result.get("result") is not True:
                        return False
                if scope["type"] == "chat":
                    result = self._request(self._token, "setChatMenuButton", {
                        "chat_id": route.channel_id, "menu_button": {"type": "commands"}})
                    if result.get("ok") is not True or result.get("result") is not True:
                        return False
            return True
        except Exception:
            return False  # Optional menu never prevents ordinary polling/commands.
