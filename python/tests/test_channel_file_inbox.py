"""File staging commits with ingress and survives reopen without executable submission."""

from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.telegram_inbox import TelegramInbox


def test_file_stage_reopen_dedup_and_body_binding(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "update-1", "read", (
        ChannelFile("telegram", "456", "2", "report.txt", 4, "text/plain"),))
    stage = ChannelFileInbox(repo)
    first = stage.stage(event, snapshot.goal.digest)
    assert first["status"] == "awaiting-file-import"
    reopened = ChannelFileInbox(RuntimeRepository(repo.root))
    assert reopened.stage(event, snapshot.goal.digest) == first
    assert reopened.pending(route) == [(event, snapshot.goal.digest)]
    assert RuntimeInbox(repo).next_pending(route.session) is None
    with pytest.raises(ContractError, match="already bound"):
        reopened.stage(replace(event, input_text="changed"), snapshot.goal.digest)
    assert reopened.pending(route) == [(event, snapshot.goal.digest)]


def test_telegram_file_staging_and_cursor_atomic(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    consumer = TelegramInbox(repo, [route], include_files=True)
    update = {"update_id": 1, "message": {"message_id": 2, "chat": {"id": 456},
              "from": {"id": 789, "is_bot": False}, "document": {
                  "file_id": "2", "file_size": 4, "file_name": "report.txt"}}}
    invalid = {"update_id": 2, "message": {"message_id": 3, "chat": {"id": 456},
               "from": {"id": 789, "is_bot": False}, "text": 123}}
    with pytest.raises(ContractError):
        consumer.admit([update, invalid], expected_offset=None)
    assert consumer.offset() is None
    assert ChannelFileInbox(repo).pending(route) == []
    result = consumer.admit([update], expected_offset=None)
    assert result["next_offset"] == 2
    assert result["submissions"][0]["status"] == "awaiting-file-import"
    reopened = RuntimeRepository(repo.root)
    assert TelegramInbox(reopened, [route], include_files=True).offset() == 2
    assert len(ChannelFileInbox(reopened).pending(route)) == 1
    assert RuntimeInbox(reopened).next_pending(route.session) is None


def test_discord_file_checkpoint_reopen(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    resume = DiscordResume(DiscordInbox(repo, [route], include_files=True))
    resume.save_ready("fixture-session", "wss://gateway.discord.gg", 1, expected=None)
    frame = {"op": 0, "t": "MESSAGE_CREATE", "s": 2, "d": {
        "id": "2", "channel_id": "456", "author": {"id": "789"}, "type": 0,
        "content": "", "attachments": [{"id": "3", "size": 4, "filename": "report.txt",
            "url": "https://cdn.discordapp.com/attachments/456/3/report.txt?ex=fixture"}]}}
    original = resume.read()
    result = resume.admit(frame, expected=original)
    assert result[0]["status"] == "awaiting-file-import"
    reopened = RuntimeRepository(repo.root)
    fresh = DiscordResume(DiscordInbox(reopened, [route], include_files=True))
    assert fresh.read()["sequence"] == 2
    assert len(ChannelFileInbox(reopened).pending(route)) == 1
    assert RuntimeInbox(reopened).next_pending(route.session) is None
    with pytest.raises(ContractError, match="checkpoint invalid"):
        fresh.admit(frame, expected=original)
    assert len(ChannelFileInbox(reopened).pending(route)) == 1
