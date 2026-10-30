"""Configured production worker with explicitly controlled HTTP and socket peers."""

import pytest
from test_discord_gateway_state import HELLO, READY
from test_discord_identify import LIMITS
from test_foreground_trusted import config_file

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_identify import DiscordIdentifyAdmission
from ahc.adapters.discord_websocket import DiscordGatewayClosed
from ahc.cli import foreground_discord
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository


@pytest.mark.parametrize("mode", ["identify", "exhausted", "resume"])
def test_composed_worker_quota_and_resume_lifecycle(tmp_path, monkeypatch, mode):
    original, request, value, _ = config_file(tmp_path, enabled=True)
    store = ThreadedRuntimeRepository(original.store.root)
    route = ChannelRoute("discord", "123", "456", "789", request.session)
    value["channels"] = [{"route": route.to_dict(), "token_env": "AHC_GATEWAY_TEST",
                           "enabled": False}]
    value["discord_gateway"] = {"enabled": True, "account_id": "123",
        "token_env": "AHC_GATEWAY_TEST", "intents": 0, "connection_limit": 2}
    repository = DiscordResume(DiscordInbox(store, [route]))
    if mode == "resume":
        repository.save_ready("old", "wss://gateway.discord.gg", 1, expected=None)
    calls, sent, closed = [], [], []

    def http(token):
        assert token == "fixture"
        calls.append(True)
        return {**LIMITS, "session_start_limit": {**LIMITS["session_start_limit"],
                                                   "remaining": 0 if mode == "exhausted" else 1}}

    class Socket:
        def __init__(self, url):
            self.frames = [HELLO, {"op": 0, "s": 2, "t": "RESUMED", "d": {}}
                           if mode == "resume" else READY]
        def receive(self, timeout):
            if self.frames:
                return self.frames.pop(0)
            raise DiscordGatewayClosed(4004)
        def send(self, frame):
            sent.append(frame["op"])
        def close(self):
            closed.append(True)

    monkeypatch.setenv("AHC_GATEWAY_TEST", "fixture")
    monkeypatch.setattr(foreground_discord, "DiscordIdentifyAdmission",
                        lambda token: DiscordIdentifyAdmission(token, request=http))
    monkeypatch.setattr(foreground_discord, "DiscordGatewayDriver",
        lambda *args, **kwargs: DiscordGatewayDriver(*args, connect=Socket, **kwargs))
    worker = foreground_discord.compose_discord(parse_foreground_config_v2(value), store)
    assert calls == sent == closed == []
    worker.start()
    try:
        worker._thread.join(timeout=3)
        assert not worker._thread.is_alive() and worker.status()["state"] == "paused"
    finally:
        worker.close()
    assert calls == ([] if mode == "resume" else [True])
    assert sent == ([] if mode == "exhausted" else [6 if mode == "resume" else 2])
    assert closed == [True]
    fresh = DiscordResume(DiscordInbox(ThreadedRuntimeRepository(store.root), [route])).read()
    if mode == "exhausted":
        assert fresh is None
    else:
        assert fresh["sequence"] == (2 if mode == "resume" else 1)
