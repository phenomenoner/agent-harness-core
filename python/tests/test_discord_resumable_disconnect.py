import json

import pytest
from test_discord_gateway_state import HELLO, READY

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_websocket import DiscordWebSocket
from ahc.domain.sessions import ContractError


class Peer:
    def __init__(self, frames):
        self.frames, self.sent, self.codes = list(frames), [], []

    def recv(self, timeout):
        if not self.frames:
            raise TimeoutError()
        value = self.frames.pop(0)
        if isinstance(value, Exception):
            raise value
        return json.dumps(value)

    def send(self, text):
        self.sent.append(json.loads(text))

    def close(self, code=1000):
        self.codes.append(code)


def test_server_reconnect_preserves_session_but_final_stop_closes_normally():
    first = Peer([HELLO, READY])
    second = Peer([HELLO, {"op": 0, "s": 2, "t": "RESUMED", "d": {}}])
    peers = iter([first, second])
    now = [0.0]

    def connect(url):
        return DiscordWebSocket(url, connect=lambda _: next(peers))

    driver = DiscordGatewayDriver("123", "fixture", 0, lambda _: None,
        connect=connect, clock=lambda: now[0], jitter=lambda: 1)
    driver.step()
    first.frames.append({"op": 7, "d": None})
    driver.step()
    assert first.codes == [4000]  # 1000 would invalidate the session at Discord.
    now[0] = 5
    driver.step()
    assert second.sent[0]["op"] == 6
    assert driver.state == "ready"
    driver.close()
    assert second.codes == [1000]


@pytest.mark.parametrize("failure", [OSError(), {"op": float('nan')}])
def test_failed_receive_uses_resumable_disconnect(failure):
    peer = Peer([failure])
    transport = DiscordWebSocket(connect=lambda _: peer)
    with pytest.raises(ContractError):
        transport.receive()
    assert peer.codes == [4000]
    transport.close()
    assert peer.codes == [4000]
