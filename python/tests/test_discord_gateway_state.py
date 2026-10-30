import pytest

from ahc.adapters.discord_gateway_state import DiscordGatewayState
from ahc.domain.sessions import ContractError

HELLO = {"op": 10, "d": {"heartbeat_interval": 45000}}
READY = {"op": 0, "s": 1, "t": "READY", "d": {"user": {"id": "123", "bot": True},
         "session_id": "fixture-session", "resume_gateway_url": "wss://gateway.discord.gg"}}


def test_login_heartbeat_durable_admission_and_resume():
    gateway = DiscordGatewayState("123", "fixture-token", 37376)
    events = []
    assert gateway.receive(HELLO, events.append)["op"] == 2
    gateway.receive(READY, events.append)
    assert gateway.heartbeat() == {"op": 1, "d": 1}
    gateway.receive({"op": 11}, events.append)
    frame = {"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": {"id": "456"}}

    def failed(frame):
        raise OSError("private-store-detail")

    with pytest.raises(ContractError) as error:
        gateway.receive(frame, failed)
    assert "private-store-detail" not in str(error.value)
    assert gateway.sequence == 1
    gateway.reconnect()
    resume = gateway.receive(HELLO, events.append)
    assert resume == {"op": 6, "d": {"token": "fixture-token",
                                      "session_id": "fixture-session", "seq": 1}}
    gateway.receive(frame, events.append)
    gateway.receive({"op": 0, "s": 3, "t": "RESUMED", "d": {}}, events.append)
    assert events == [frame] and gateway.state == "ready"
    gateway.heartbeat()
    assert gateway.heartbeat() == {"action": "reconnect"}


def test_wrong_account_cannot_admit_events():
    gateway = DiscordGatewayState("999", "fixture", 0)
    events = []
    gateway.receive(HELLO, events.append)
    with pytest.raises(ContractError):
        gateway.receive(READY, events.append)
    assert events == [] and gateway.sequence is None
