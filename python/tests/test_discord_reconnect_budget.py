import pytest
from test_discord_gateway_state import HELLO, READY

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_websocket import DiscordGatewayClosed


def fixture_driver():
    now, peers = [0.0], []

    class Socket:
        def __init__(self, url):
            index = len(peers)
            self.frames = [HELLO, READY if index == 0 else
                           {"op": 0, "s": index + 1, "t": "RESUMED", "d": {}}]
            self.sent = []
            peers.append(self)

        def receive(self, timeout):
            item = self.frames.pop(0) if self.frames else None
            if isinstance(item, Exception):
                raise item
            return item

        def send(self, frame):
            self.sent.append(frame)

        def close(self):
            pass

    return DiscordGatewayDriver("123", "fixture", 0, lambda _: None,
        connect=Socket, clock=lambda: now[0], jitter=lambda: 0), now, peers


def healthy(driver, now, peer):
    peer.frames.append({"op": 11})
    driver.step()
    now[0] += 45
    driver.step()
    now[0] += 15
    peer.frames.append({"op": 11})
    driver.step()


def test_two_healthy_disconnects_do_not_exhaust_lifetime_budget():
    driver, now, peers = fixture_driver()
    driver.step()
    for _ in range(2):
        healthy(driver, now, peers[-1])
        peers[-1].frames.append({"op": 7, "d": None})
        driver.step()
        assert driver.state == "reconnecting"
        now[0] += 5
        driver.step()
        assert driver.state == "ready"
    assert len(peers) == 3
    assert all(peer.sent[0]["op"] == 6 for peer in peers[1:])
    assert driver.status()["total_connection_attempts"] == 3
    driver.close()


def test_ready_flapping_waits_for_cooldown_before_new_burst():
    driver, now, peers = fixture_driver()
    driver.step()
    for _ in range(2):
        peers[-1].frames.extend([{"op": 11}, {"op": 7, "d": None}])
        driver.step()
        now[0] += 5
        driver.step()
    assert driver.state == "reconnecting" and len(peers) == 2
    now[0] = 64
    driver.step()
    assert len(peers) == 2
    now[0] = 65
    driver.step()
    assert driver.state == "ready" and len(peers) == 3


def test_unsolicited_ack_after_stability_time_cannot_reset_budget():
    driver, now, peers = fixture_driver()
    driver.step()
    peers[0].frames.append({"op": 11})
    driver.step()
    now[0] = 60
    peers[0].frames.append({"op": 11})
    driver.step()
    assert driver.status()["connection_attempts"] == 1


def test_transient_failure_burst_cools_down_then_resumes_without_replaying_input():
    driver, now, peers = fixture_driver()
    driver.step()
    for _ in range(2):
        peers[-1].frames.append({"op": 7, "d": None})
        driver.step()
        now[0] += 5
        driver.step()
    assert len(peers) == 2 and driver.state == "reconnecting"
    now[0] += 60
    driver.step()
    assert len(peers) == 3 and driver.state == "ready"
    assert peers[-1].sent[0]["op"] == 6
    assert driver.status()["total_connection_attempts"] == 3
    driver.close()


@pytest.mark.parametrize("code", [4004, 4010, 4011, 4012, 4013, 4014])
def test_fatal_close_still_pauses_after_healthy_reset(code):
    driver, now, peers = fixture_driver()
    driver.step()
    healthy(driver, now, peers[0])
    peers[0].frames.append(DiscordGatewayClosed(code))
    driver.step()
    now[0] += 100
    driver.step()
    assert driver.state == "paused" and len(peers) == 1
    assert driver.status()["reason"] == "non-reconnectable-close"
