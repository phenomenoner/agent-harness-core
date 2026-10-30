"""Typing API acknowledgements with no real platform request."""

import json

import pytest
from test_runtime_core import setup

from ahc.adapters import channel_typing
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("fault", [None, "status", "payload", "oversize", "request"])
def test_typing_http_acknowledgement_and_cleanup(tmp_path, monkeypatch, platform, fault):
    _, snapshot, _ = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    events = []

    class Response:
        status = 429 if fault == "status" else 200 if platform == "telegram" else 204

        def read(self, limit):
            assert limit == 4097
            if fault == "oversize":
                return b" " * 4097
            if fault == "payload":
                return b'{"ok":true,"result":false}'
            return b'{"ok":true,"result":true}' if platform == "telegram" else b""

    class Connection:
        def __init__(self, host, timeout):
            assert host == ("api.telegram.org" if platform == "telegram" else "discord.com")
            assert timeout == 5

        def request(self, method, path, body, headers):
            events.append("request")
            assert method == "POST"
            if platform == "telegram":
                assert path == "/botfixture/sendChatAction"
                assert json.loads(body) == {"chat_id": "456", "action": "typing"}
                assert "Authorization" not in headers
            else:
                assert path == "/api/v10/channels/456/typing" and body == b""
                assert headers["Authorization"] == "Bot fixture"
            if fault == "request":
                raise OSError("fixture-private")

        def getresponse(self):
            return Response()

        def close(self):
            events.append("close")

    monkeypatch.setattr(channel_typing.http.client, "HTTPSConnection", Connection)
    transport = channel_typing.BotTypingTransport(route, "fixture")
    if fault:
        with pytest.raises(ContractError, match="typing request unverified") as error:
            transport.send()
        assert "fixture-private" not in str(error.value)
    else:
        assert transport.send() == {"status": "acknowledged", "kind": "typing"}
    assert events == ["request", "close"]
