from test_discord_gateway_state import HELLO, READY

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver


def test_pump_heartbeat_ack_and_bounded_resume():
    now, peers, events = [0.0], [], []

    class Socket:
        def __init__(self, url):
            self.frames, self.sent, self.closed = [HELLO, READY], [], False
            peers.append(self)
            if len(peers) >= 2:
                self.frames = [HELLO, {"op": 0, "s": len(peers), "t": "RESUMED", "d": {}}]

        def receive(self, timeout):
            return self.frames.pop(0) if self.frames else None

        def send(self, frame):
            self.sent.append(frame)

        def close(self):
            self.closed = True

    driver = DiscordGatewayDriver("123", "fixture", 0, events.append,
                                  connect=Socket, clock=lambda: now[0], jitter=lambda: 0)
    driver.step()
    assert [f["op"] for f in peers[0].sent] == [2, 1]
    peers[0].frames = [{"op": 1}, {"op": 11}]
    driver.step()
    assert peers[0].sent[-1]["op"] == 1 and not peers[0].closed
    now[0] = 45
    driver.step()
    now[0] = 90
    driver.step()
    assert peers[0].closed and driver.state == "reconnecting"
    driver.step()
    assert len(peers) == 1
    now[0] = 95
    driver.step()
    assert peers[1].sent[0]["op"] == 6
    now[0] = 140
    driver.step()
    assert peers[1].closed and driver.state == "reconnecting"
    now[0] = 200
    driver.step()
    assert len(peers) == 3 and driver.state == "ready" and events == []
    driver.close()
    assert driver.state == "closed"
