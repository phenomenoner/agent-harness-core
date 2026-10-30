"""Recovery config, consumer rebind, receive/turn/delivery and reopened supervisor."""

import json
from dataclasses import replace

import pytest
from discord_receive_fixture import configure
from test_channel_recovery_dedup import recover
from test_foreground_config_v2 import valid_config
from test_restore_interpreter_scenario import KernelPort
from test_runtime_core import setup
from test_telegram_inbox import update

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.adapters.channel_text import BotTextTransport
from ahc.adapters.telegram_poll import TelegramPoll
from ahc.adapters.workspace_kernel import KERNEL
from ahc.cli import foreground_channels, foreground_telegram, foreground_trusted, main
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.runtime_recovery_config import prepare_recovered_config
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelRoute, channel_operation_id
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_recovery_binding import rebind_recovered_consumer
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.telegram_inbox import TelegramInbox


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_recovered_channel_run_delivery_and_reopen(tmp_path, monkeypatch, capsys, platform):
    repo, saved, _ = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", saved.session)
    config = valid_config()
    config["runtime_root"] = str(repo.root)
    config["channels"] = [{"route": route.to_dict(), "token_env": "RECOVERY_FIXTURE",
                           "enabled": False}]
    original = tmp_path / "original.json"
    original.write_text(json.dumps(config), encoding="utf-8")
    digest = parse_foreground_config_v2(config).configuration_digest
    message = {"id": "10", "channel_id": "456", "author": {"id": "789"},
               "type": 0, "content": "old"}
    if platform == "telegram":
        old = TelegramInbox(repo, [route])
        old.admit([update(1, text="old")], expected_offset=None)
        position = old.offset()
    else:
        old = DiscordResume(DiscordInbox(repo, [route]))
        old.save_ready("fixture", "wss://gateway.discord.gg", 1, expected=None)
        old.admit({"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": message}, expected=old.read())
        position = old.read()
    successor = recover(repo, saved, tmp_path)
    destination = tmp_path / "config"
    prepare_recovered_config(repo, original, digest, destination,
                              checkpoint_digest=None, enable_backend=True)
    path = destination / "foreground.json"
    document = json.loads(path.read_text())
    assert not document["channels"][0]["enabled"]
    new_route = replace(route, session=successor)
    rebind_recovered_consumer(repo, tmp_path / "supervisor.lock", [route], [new_route],
                              expected=position)
    # Explicit fixture-only reactivation; generated config did not start these effects.
    document["channels"][0]["enabled"] = True
    monkeypatch.setenv("RECOVERY_FIXTURE", "fixture")
    peers, offsets = [], []
    if platform == "discord":
        peers = configure(document, path, monkeypatch,
                          messages=[message, {**message, "id": "11", "content": "new"}])
    else:
        document["telegram_polling"] = {"account_id": "123", "token_env": "RECOVERY_FIXTURE",
            "enabled": True, "exclusive_consumer": True, "interval_seconds": 1}

        def poll(token, method, body):
            if method == "getMe":
                return {"ok": True, "result": {"id": 123, "is_bot": True}}
            offsets.append(body["offset"])
            return {"ok": True, "result": [update(2, text="new")] if body["offset"] == 2 else []}

        monkeypatch.setattr(foreground_telegram, "TelegramPoll",
                            lambda a, t, **options: TelegramPoll(a, t, request=poll, **options))
    path.write_text(json.dumps(document), encoding="utf-8")
    calls, sent, ports = [], [], []

    def post(platform, token, channel, body):
        sent.append(body)
        if platform == "telegram":
            return {"ok": True, "result": {"message_id": 20, "chat": {"id": 456},
                                            "from": {"id": 123}}}
        return {"id": "20", "channel_id": "456", "author": {"id": "123"}}

    class Factory:
        def __init__(self, store, config):
            self.config = config

        def recover(self, *_):
            return ()

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, bound, tools, *, cancelled):
            class Backend:
                def invoke(self, admitted):
                    calls.append(admitted.operation_id)
                    assert admitted.input_text == "new"
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                output_text="new reply")
            return Backend()

    def workspace(store, runner, identity, **options):
        port = KernelPort(identity, options["checkpoint"], KERNEL)
        ports.append(port)
        return port

    monkeypatch.setattr(foreground_channels, "BotTextTransport",
                        lambda r, t: BotTextTransport(r, t, post=post))
    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", lambda *a, **kw: object())
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace", workspace)
    app = tmp_path / "app.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    for _ in range(2):
        assert main(["--config", str(app), "run", "--runtime-config", str(path),
                     "--max-runtime", "2"]) == 0
        assert json.loads(capsys.readouterr().out)["runtime"]["state"] == "closed"
    operation = channel_operation_id(
        new_route, "update-2" if platform == "telegram" else "message-11")
    historical = channel_operation_id(route, "update-1" if platform == "telegram" else "message-10")
    assert calls == [operation] and len(sent) == 1
    assert RuntimeInbox(repo).query(saved.session, historical)["status"] == "pending"
    assert RuntimeInbox(repo).query(successor, operation)["status"] == "terminal"
    assert ChannelOutbox(repo).read(new_route)[0]["state"] == "sent"
    assert len(ports) == 2 and all(port.process.returncode == 0 for port in ports)
    assert all(peer.closed and not peer.failures for peer in peers)
    if platform == "telegram":
        assert offsets[0] == 2 and offsets[-1] == 3
