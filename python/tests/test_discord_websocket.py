import json

import pytest

from ahc.adapters.discord_websocket import DiscordWebSocket
from ahc.domain.sessions import ContractError


class Connection:
    def __init__(self):
        self.closed, self.sent, self.value = 0, [], '{"op":10}'

    def send(self, text):
        self.sent.append(json.loads(text))

    def recv(self, timeout):
        if isinstance(self.value, Exception):
            raise self.value
        return self.value

    def close(self, code=1000):
        self.closed += 1


def test_bounded_frames_timeout_and_cleanup():
    connection, urls = Connection(), []

    def connect(url):
        urls.append(url)
        return connection

    peer = DiscordWebSocket(connect=connect)
    assert urls == ["wss://gateway.discord.gg/?v=10&encoding=json"]
    peer.send({"op": 1, "d": None})
    assert connection.sent == [{"op": 1, "d": None}]
    assert peer.receive() == {"op": 10}
    connection.value = TimeoutError()
    assert peer.receive() is None and connection.closed == 0
    with pytest.raises(ContractError):
        peer.send({"d": "x" * 4096})
    assert len(connection.sent) == 1
    connection.value = '{"op":0,"op":1}'
    with pytest.raises(ContractError):
        peer.receive()
    assert connection.closed == 1
    peer.close()
    assert connection.closed == 1


@pytest.mark.parametrize("url", ["ws://gateway.discord.gg", "wss://example.com",
    "wss://gateway.discord.gg@evil.test", "wss://gateway.discord.gg/path",
    "wss://gateway.discord.gg?token=secret"])
def test_invalid_destination_never_connects(url):
    with pytest.raises(ContractError):
        DiscordWebSocket(url, connect=lambda url: pytest.fail("unexpected connect"))
