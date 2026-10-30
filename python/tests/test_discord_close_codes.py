import pytest
from test_discord_gateway_state import HELLO, READY

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_websocket import DiscordGatewayClosed, DiscordWebSocket


@pytest.mark.parametrize("code", [4004, 4010, 4011, 4012, 4013, 4014, 4007, 4009, 1000, 4000])
def test_peer_close_stops_or_invalidates_session_before_reconnect(code):
    now, peers, checkpoints = [0], [], []

    class Socket:
        def __init__(self, url):
            self.frames, self.sent = [HELLO, READY], []
            self.closed = False
            peers.append(self)

        def receive(self, timeout):
            if self.frames:
                return self.frames.pop(0)
            raise DiscordGatewayClosed(code)

        def send(self, frame):
            self.sent.append(frame)

        def close(self, code=1000):
            self.closed = True

    driver = DiscordGatewayDriver("123", "fixture", 0, lambda _: None, connect=Socket,
        clock=lambda: now[0], checkpoint=checkpoints.append)
    driver.step()
    assert peers[0].closed and driver.status()["close_code"] == code
    reset = code in {4007, 4009, 1000}
    fatal = code in {4004, 4010, 4011, 4012, 4013, 4014}
    if reset:
        assert checkpoints[-1] == {"op": 9, "d": False}
        assert driver.protocol.session_id is None
    now[0] = 10
    driver.step()
    if fatal:
        assert len(peers) == 1 and driver.state == "paused"
    else:
        assert len(peers) == 2
        assert peers[1].sent[0]["op"] == (2 if reset else 6)
    driver.close()


def test_failed_checkpoint_invalidation_pauses_without_new_identify():
    def checkpoint(frame):
        if frame["op"] == 9:
            raise RuntimeError("private storage failure")

    class Socket:
        def receive(self, timeout):
            raise DiscordGatewayClosed(4009)

        def close(self):
            pass

    driver = DiscordGatewayDriver("123", "fixture", 0, lambda _: None,
        connect=lambda _: Socket(), checkpoint=checkpoint)
    driver.step()
    assert driver.state == "paused"


@pytest.mark.parametrize("operation", ["receive", "send"])
def test_transport_preserves_only_typed_peer_code(operation):
    pytest.importorskip("websockets")
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close

    class Connection:
        def recv(self, **kwargs):
            raise ConnectionClosedError(Close(4014, "private reason"), None)

        def send(self, text):
            self.recv()

        def close(self, code=1000):
            pass

    socket = DiscordWebSocket(connect=lambda _: Connection())
    with pytest.raises(DiscordGatewayClosed) as caught:
        if operation == "receive":
            socket.receive()
        else:
            socket.send({"op": 1, "d": None})
    assert caught.value.code == 4014
    assert "private" not in str(caught.value)
