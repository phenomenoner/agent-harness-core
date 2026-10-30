from copy import deepcopy

import pytest
from test_discord_gateway_state import HELLO

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_identify import DiscordIdentifyAdmission

LIMITS = {"shards": 1, "session_start_limit": {
    "total": 1000, "remaining": 2, "reset_after": 10000, "max_concurrency": 1}}


def test_fresh_limits_cooldown_and_exhaustion_without_constructor_effect():
    calls, clock = [], [0]
    def request(token):
        calls.append(True)
        return LIMITS if len(calls) == 1 else {**LIMITS,
            "session_start_limit": {**LIMITS["session_start_limit"], "remaining": 0}}
    admission = DiscordIdentifyAdmission("fixture", request=request, clock=lambda: clock[0])
    assert calls == []
    assert admission()
    assert not admission() and len(calls) == 1
    clock[0] = 5
    assert not admission() and admission.reason == "identify-quota-exhausted"
    assert len(calls) == 2


@pytest.mark.parametrize("field,value", [("remaining", True), ("remaining", -1),
    ("remaining", 1001), ("max_concurrency", 0), ("reset_after", -1)])
def test_invalid_limits_reject_without_private_error(field, value):
    document = deepcopy(LIMITS)
    document["session_start_limit"][field] = value
    admission = DiscordIdentifyAdmission("fixture", request=lambda _: document)
    assert not admission() and admission.reason == "identify-preflight-unavailable"


@pytest.mark.parametrize("resume", [False, True])
def test_driver_never_sends_denied_identify_and_does_not_charge_resume(resume):
    calls, sent = [], []
    class Socket:
        def __init__(self, url):
            self.frames = [HELLO]
        def receive(self, timeout):
            return self.frames.pop() if self.frames else None
        def send(self, frame):
            sent.append(frame["op"])
        def close(self):
            pass
    def reject():
        calls.append(True)
        return False
    driver = DiscordGatewayDriver("123", "fixture", 0, lambda _: None, connect=Socket,
        jitter=lambda: 1, identify_admission=reject,
        resume={"session_id": "old", "sequence": 1, "resume_url": "wss://gateway.discord.gg"}
        if resume else None)
    driver.step()
    assert sent == ([6] if resume else [])
    assert calls == ([] if resume else [True])
    if not resume:
        assert driver.state == "paused"
        driver.step()
        assert calls == [True]
    driver.close()


def test_http_adapter_fixed_endpoint_bound_read_and_close(monkeypatch):
    import json

    from ahc.adapters import discord_identify

    calls = []
    class Connection:
        def __init__(self, host, timeout):
            assert host == "discord.com" and timeout == 10
        def request(self, method, path, *, headers):
            assert method == "GET" and path == "/api/v10/gateway/bot"
            assert headers == {"Authorization": "Bot fixture"}
        def getresponse(self):
            return self
        status = 200
        def read(self, limit):
            assert limit == 65537
            return json.dumps(LIMITS).encode()
        def close(self):
            calls.append("closed")
    monkeypatch.setattr(discord_identify.http.client, "HTTPSConnection", Connection)
    assert DiscordIdentifyAdmission("fixture")()
    assert calls == ["closed"]


def test_cancellation_during_identify_preflight_prevents_send():
    stopped, sent = [], []
    class Socket:
        def __init__(self, url):
            pass
        def receive(self, timeout):
            return HELLO
        def send(self, frame):
            sent.append(frame)
        def close(self):
            pass
    def admitted():
        stopped.append(True)
        return True
    driver = DiscordGatewayDriver("123", "fixture", 0, lambda _: None, connect=Socket,
                                  identify_admission=admitted)
    driver.step(cancelled=lambda: bool(stopped))
    assert sent == [] and driver.state == "paused"
