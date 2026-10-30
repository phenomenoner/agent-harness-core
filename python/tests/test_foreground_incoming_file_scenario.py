"""Configured CLI supervisor receives, imports, consumes and deduplicates files."""

import base64
import json

import pytest
from discord_receive_fixture import configure
from test_foreground_trusted import config_file
from test_workspace_tools import Port

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.adapters.channel_file_download import BotFileDownload
from ahc.adapters.telegram_poll import TelegramPoll
from ahc.cli import foreground_files, foreground_telegram, foreground_trusted, main
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_incoming_file_run_reopen_no_replay(tmp_path, monkeypatch, capsys, platform):
    original, request, config, path = config_file(tmp_path, enabled=True)
    app = tmp_path / "ahc.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    route = ChannelRoute(platform, "123", "456", "789", request.session)
    config["channels"] = [{"route": route.to_dict(), "token_env": "INCOMING_TEST",
                           "enabled": False}]
    if platform == "discord":
        peers = configure(config, path, monkeypatch, attachments=[{
            "id": "3", "filename": "report.txt", "size": 4, "content_type": "text/plain",
            "url": "https://cdn.discordapp.com/attachments/456/3/report.txt?ex=fixture"}])
        config["discord_gateway"]["include_files"] = True
    else:
        peers = []
        config["telegram_polling"] = {"account_id": "123", "token_env": "INCOMING_TEST",
            "enabled": True, "exclusive_consumer": True, "interval_seconds": 1,
            "include_files": True}
        monkeypatch.setenv("INCOMING_TEST", "fixture")

        def poll(token, method, body):
            if method == "getMe":
                return {"ok": True, "result": {"id": 123, "is_bot": True}}
            return {"ok": True, "result": [] if body.get("offset") else [{
                "update_id": 1, "message": {"message_id": 2, "chat": {"id": 456},
                    "from": {"id": 789, "is_bot": False}, "caption": "hello channel",
                    "document": {"file_id": "3", "file_name": "report.txt",
                                 "file_size": 4, "mime_type": "text/plain"}}}]}

        monkeypatch.setattr(foreground_telegram, "TelegramPoll",
                            lambda a, t, **options: TelegramPoll(a, t, request=poll, **options))
    path.write_text(json.dumps(config), encoding="utf-8")
    downloads, calls = [], []

    def fetch(host, method, path, body, limit):
        if method == "POST":
            result = {"file_id": "3", "file_size": 4, "file_path": "documents/report.txt"}
            return json.dumps({"ok": True, "result": result}).encode()
        downloads.append(host)
        return b"data"

    monkeypatch.setattr(foreground_files, "BotFileDownload",
                        lambda r, t: BotFileDownload(r, t, request=fetch))

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
                    calls.append(admitted.operation_id)
                    assert admitted.input_text == "hello channel"
                    expected = tools.workspace.identity.to_dict()
                    listed = tools.invoke("workspace.incoming_artifacts", {"expected": expected})
                    assert listed["disposition"] == "observed"
                    assert len(listed["result"]) == 1
                    selected = listed["result"][0]
                    assert selected["filename"] == "report.txt"
                    read = tools.invoke("workspace.read_artifact", {"expected": expected,
                        "artifact_id": selected["artifact_id"], "offset": 0, "length": 4096})
                    assert base64.b64decode(read["result"]["data"]) == b"data"
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                output_text="read data")
            return Backend()

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", lambda *a, **kw: object())
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                        lambda store, runner, identity, **kw: Port(identity, None))
    for _ in range(2):
        assert main(["--config", str(app), "run", "--runtime-config", str(path),
                     "--max-runtime", "3" if platform == "discord" else "1"]) == 0
        report = json.loads(capsys.readouterr().out)["runtime"]
        assert report["state"] == "closed"
        assert report["channels"]["files"]["failed_events"] == 0
        assert ChannelFileInbox(RuntimeRepository(original.store.root)).pending(route) == []
    assert len(calls) == len(downloads) == 1
    assert all(peer.closed and not peer.failures for peer in peers)
