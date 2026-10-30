"""Configured idle file processing with real import and submission storage."""

from types import SimpleNamespace

import pytest
from test_artifacts import state

from ahc.cli import foreground_files
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("failure", [False, True])
def test_idle_file_import_and_retirement(tmp_path, monkeypatch, platform, failure):
    repo, snapshot, producer, _ = state(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    reference = ChannelFile(platform, "456", "2", "report.txt", 4, "text/plain",
                            None if platform == "telegram" else
                            "https://cdn.discordapp.com/attachments/456/2/report.txt")
    event = ChannelInboundEvent(route, "event-1", "read", (reference,))
    ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
    key = "telegram_polling" if platform == "telegram" else "discord_gateway"
    document = {"channels": [{"route": route.to_dict()}], key: {
        "enabled": True, "include_files": True, "account_id": "123", "token_env": "FILE_TEST"}}
    monkeypatch.setenv("FILE_TEST", "fixture-token")
    calls = []

    class Download:
        def __init__(self, bound, token):
            assert bound == route and token == "fixture-token"

        def fetch(self, ref):
            calls.append(ref)
            if failure:
                raise RuntimeError("private URL must not escape")
            return b"data"

    monkeypatch.setattr(foreground_files, "BotFileDownload", Download)
    process = foreground_files.compose_files(SimpleNamespace(to_dict=lambda: document), repo)
    workspace = SimpleNamespace(identity=producer, store=repo, closed=False)
    process(workspace, cancelled=lambda: True)
    assert calls == []
    process(workspace, cancelled=lambda: False)
    process(workspace, cancelled=lambda: False)
    assert calls == [reference]
    assert process.status()["state"] == ("degraded" if failure else "ready")
    pending = RuntimeInbox(repo).next_pending(route.session)
    if failure:
        assert pending is None and len(ChannelFileInbox(repo).pending(route)) == 1
    else:
        assert pending["operation_id"] == event.operation_id
        assert pending["input_text"] == "read"
        assert ChannelFileInbox(repo).pending(route) == []
        assert ChannelFileInbox(repo).stage(event, snapshot.goal.digest)["event_digest"]
        assert ChannelFileInbox(repo).pending(route) == []
