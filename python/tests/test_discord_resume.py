import copy

import pytest
from test_runtime_core import setup

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume, DiscordResumeSession
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_checkpoint_and_message_commit_atomically(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    resume = DiscordResume(DiscordInbox(repo, [route]))
    resume.save_ready("gateway-session", "wss://gateway.discord.gg", 1, expected=None)
    before = resume.read()
    frame = {"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": {"id": "10", "channel_id": "456",
             "author": {"id": "789"}, "type": 0, "content": "persist"}}
    bad = copy.deepcopy(frame)
    bad["d"]["content"] = 123
    with pytest.raises(ContractError):
        resume.admit(bad, expected=before)
    assert resume.read() == before
    assert RuntimeInbox(repo).next_pending(route.session) is None
    assert len(resume.admit(frame, expected=before)) == 1
    reopened = DiscordResume(DiscordInbox(RuntimeRepository(repo.root), [route]))
    assert reopened.read() == {**before, "sequence": 2}
    assert RuntimeInbox(repo).next_pending(route.session)["input_text"] == "persist"
    with pytest.raises(ContractError):
        reopened.admit(frame, expected=before)
    with pytest.raises(ContractError):
        reopened.save_ready("other", "wss://evil.example", 3, expected=reopened.read())


def test_invalid_session_clears_checkpoint_before_fresh_identify(tmp_path):
    from test_discord_gateway_state import HELLO, READY

    from ahc.adapters.discord_gateway_state import DiscordGatewayState

    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    repository = DiscordResume(DiscordInbox(repo, [route]))
    session = DiscordResumeSession(repository)
    gateway = DiscordGatewayState("123", "fixture", 0, checkpoint=session.checkpoint)
    gateway.receive(HELLO, session.admit)
    gateway.receive(READY, session.admit)
    assert repository.read()["sequence"] == 1
    gateway.receive({"op": 9, "d": False}, session.admit)
    assert repository.read() is None and session.current is None
    gateway.reconnect()
    assert gateway.receive(HELLO, session.admit)["op"] == 2
