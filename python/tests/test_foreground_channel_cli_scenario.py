"""Real CLI supervisor/inbox/outbox with controlled model and workspace peers."""

import http.client
import json

import pytest
from discord_receive_fixture import configure as configure_discord
from test_foreground_trusted import config_file
from test_workspace_tools import Port

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.adapters.channel_text import BotTextTransport
from ahc.adapters.telegram_poll import TelegramPoll
from ahc.cli import foreground_channels, foreground_telegram, main
from ahc.cli import foreground_trusted as composition
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelRoute, channel_operation_id
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.fixture(autouse=True)
def no_external_http(monkeypatch):
    attempts = []
    def blocked(*args, **kwargs):
        attempts.append(True)
        raise AssertionError("controlled channel scenario attempted external HTTP")
    monkeypatch.setattr(http.client.HTTPSConnection, "request", blocked)
    yield
    assert not attempts, "controlled channel scenario attempted external HTTP"


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize("incoming", ["cli", "poll"])
def test_channel_submit_run_outbox_and_restart_without_replay(tmp_path, monkeypatch, capsys,
                                                            platform, automatic, incoming):
    original, request, config, path = config_file(tmp_path, enabled=True)
    app = tmp_path / "ahc.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    route = ChannelRoute(platform, "123", "456", "789", request.session)
    config["channels"] = [{"route": route.to_dict(), "token_env": "AHC_CHANNEL_FIXTURE",
                           "enabled": automatic}]
    path.write_text(json.dumps(config), encoding="utf-8")
    sent = []

    def post(platform, token, channel, body):
        assert automatic and token == "fixture-token"
        sent.append(body)
        if platform == "telegram":
            return {"ok": True, "result": {"message_id": 1, "chat": {"id": 456},
                                            "from": {"id": 123}}}
        return {"id": "1", "channel_id": "456", "author": {"id": "123"}}

    monkeypatch.setattr(foreground_channels, "BotTextTransport",
                        lambda route, token: BotTextTransport(route, token, post=post))
    monkeypatch.delenv("AHC_CHANNEL_FIXTURE", raising=False)
    if automatic:
        monkeypatch.setenv("AHC_CHANNEL_FIXTURE", "fixture-token")
    common = ["--runtime-root", str(original.store.root), "--route-json",
              json.dumps(route.to_dict())]
    offsets = []
    discord_peers = []
    if incoming == "cli":
        assert main(["runtime-channel-submit", *common, "--event-id", "event-1",
                     "--goal-digest", request.goal_digest, "--input-text", "hello channel"]) == 0
        operation = json.loads(capsys.readouterr().out)["submission"]["operation_id"]
    elif platform == "discord":
        discord_peers = configure_discord(config, path, monkeypatch)
        operation = channel_operation_id(route, "message-10")
    else:
        config["telegram_polling"] = {"account_id": "123", "token_env": "AHC_POLL_FIXTURE",
            "enabled": True, "exclusive_consumer": True, "interval_seconds": 1}
        path.write_text(json.dumps(config), encoding="utf-8")
        monkeypatch.setenv("AHC_POLL_FIXTURE", "poll-fixture")
        operation = channel_operation_id(route, "update-1")

        def request_poll(token, method, body):
            assert method in ("getMe", "getUpdates")
            if method == "getMe":
                return {"ok": True, "result": {"id": 123, "is_bot": True}}
            offsets.append(body.get("offset"))
            rows = [{"update_id": 1, "message": {"message_id": 2, "chat": {"id": 456},
                     "from": {"id": 789, "is_bot": False}, "text": "hello channel"}}]
            return {"ok": True, "result": rows if body.get("offset") is None else []}

        monkeypatch.setattr(foreground_telegram, "TelegramPoll",
                            lambda account, token, **options: TelegramPoll(
                                account, token, request=request_poll, **options))
    calls = []

    class Runner:
        def __init__(self, *args, **kwargs):
            pass

    class Factory:
        def __init__(self, store, config):
            self.config = config

        def recover(self, *args):
            return ()

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, bound, tools, *, cancelled):
            class Backend:
                def invoke(self, admitted):
                    assert admitted.operation_id == operation
                    assert admitted.session == request.session
                    assert admitted.input_text == "hello channel"
                    calls.append(admitted.operation_id)
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                "thread", "turn", "channel answer")
            return Backend()

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", Runner)
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                        lambda store, runner, identity, **kw: Port(identity, None))
    for _ in range(2):
        assert main(["--config", str(app), "run", "--runtime-config", str(path),
                     "--max-runtime", "3" if discord_peers or (
                         platform == "discord" and incoming == "poll") else "1"]) == 0
        report = json.loads(capsys.readouterr().out)["runtime"]
        assert report["state"] == "closed"
        if platform == "discord" and incoming == "poll":
            assert report["channels"]["receiver"]["state"] == "closed", report
            assert all(not peer.failures for peer in discord_peers), [
                peer.failures for peer in discord_peers]
        assert main(["runtime-channel-outbox", *common]) == 0
        rows = json.loads(capsys.readouterr().out)["outbox"]
        assert len(rows) == 1
        assert rows[0]["route"] == route.to_dict()
        assert rows[0]["state"] == ("sent" if automatic else "pending")
        assert rows[0]["payload"] == {"output_text": "channel answer"}
    assert calls == [operation]
    assert len(sent) == int(automatic)
    if incoming == "poll" and platform == "telegram":
        assert offsets[0] is None and offsets[1:] and set(offsets[1:]) == {2}
    if discord_peers:
        assert len(discord_peers) == 2 and all(peer.closed for peer in discord_peers)
        assert discord_peers[0].sent[0]["op"] == 2
        assert discord_peers[1].sent[0]["op"] == 6
        assert discord_peers[1].sent[0]["d"]["seq"] == 2
    reopened = RuntimeRepository(original.store.root)
    assert RuntimeInbox(reopened).next_pending(request.session) is None
