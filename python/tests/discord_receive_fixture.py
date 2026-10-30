"""Controlled transport for the real Discord driver/worker/SQLite composition."""

import json

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_identify import DiscordIdentifyAdmission
from ahc.cli import foreground_discord


def configure(config, path, monkeypatch, *, attachments=None, messages=None):
    config["discord_gateway"] = {"account_id": "123", "token_env": "AHC_DISCORD_FIXTURE",
        "enabled": True, "intents": 37376, "connection_limit": 2}
    path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setenv("AHC_DISCORD_FIXTURE", "discord-fixture")
    peers = []
    failures = []

    def identify_limits(token):
        assert token == "discord-fixture"
        return {"shards": 1, "session_start_limit": {
            "total": 100, "remaining": 100, "reset_after": 60000, "max_concurrency": 1}}

    monkeypatch.setattr(foreground_discord, "DiscordIdentifyAdmission",
                        lambda token: DiscordIdentifyAdmission(token, request=identify_limits))

    class Socket:
        def __init__(self, url):
            self.closed = False
            self.failures = failures
            self.sent = []
            peers.append(self)
            self.frames = [{"op": 10, "d": {"heartbeat_interval": 45000}}]

        def send(self, frame):
            self.sent.append(frame)
            if frame["op"] == 1:
                self.frames.append({"op": 11})
            elif frame["op"] in (2, 6):
                sequence = 1 if frame["op"] == 2 else frame["d"]["seq"] + 1
                self.frames.append({"op": 0, "s": sequence,
                    "t": "READY" if frame["op"] == 2 else "RESUMED",
                    "d": {"user": {"id": "123", "bot": True}, "session_id": "fixture",
                          "resume_gateway_url": "wss://gateway.discord.gg"}})
                defaults = [{
                    "id": "10", "channel_id": "456", "author": {"id": "789"},
                    "type": 0, "content": "hello channel",
                    **({"attachments": attachments} if attachments is not None else {})}]
                for ordinal, message in enumerate(defaults if messages is None else messages, 1):
                    self.frames.append({"op": 0, "s": sequence + ordinal,
                                        "t": "MESSAGE_CREATE", "d": message})

        def receive(self, timeout):
            return self.frames.pop(0) if self.frames else None

        def close(self):
            self.closed = True

    def driver(account, token, intents, admit, **options):
        checkpoint = options.get("checkpoint")
        if checkpoint is not None:
            def record_checkpoint(frame):
                try:
                    return checkpoint(frame)
                except Exception as error:
                    failures.append((type(error).__name__, str(error)))
                    raise
            options["checkpoint"] = record_checkpoint
        def record(frame):
            try:
                return admit(frame)
            except Exception as error:
                failures.append((type(error).__name__, str(error)))
                raise
        return DiscordGatewayDriver(account, token, intents, record, **options, connect=Socket)

    monkeypatch.setattr(foreground_discord, "DiscordGatewayDriver", driver)
    return peers
