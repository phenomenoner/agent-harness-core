import copy

import pytest
from test_discord_gateway_state import HELLO, READY
from test_runtime_core import setup

from ahc.adapters.discord_gateway_state import DiscordGatewayState
from ahc.domain.channel_ingress import ChannelRoute, channel_operation_id
from ahc.domain.sessions import ContractError
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_gateway_sequence_follows_durable_admission_and_dedup_survives_reopen(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    consume = DiscordInbox(repo, [route])
    gateway = DiscordGatewayState("123", "fixture", 0)
    gateway.receive(HELLO, consume)
    gateway.receive(READY, consume)
    frame = {"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": {"id": "10", "channel_id": "456",
             "author": {"id": "789"}, "type": 0, "content": "persist me"}}
    gateway.receive(frame, consume)
    assert gateway.sequence == 2
    operation = channel_operation_id(route, "message-10")
    inbox = RuntimeInbox(RuntimeRepository(repo.root))
    assert inbox.next_pending(route.session)["operation_id"] == operation
    reopened = DiscordInbox(RuntimeRepository(repo.root), [route])
    second = DiscordGatewayState("123", "fixture", 0)
    second.receive(HELLO, reopened)
    second.receive(READY, reopened)
    second.receive(frame, reopened)
    with repo._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1
    changed = copy.deepcopy(frame)
    changed["s"] = 3
    changed["d"]["content"] = "changed body"
    with pytest.raises(ContractError):
        second.receive(changed, reopened)
    assert second.sequence == 2
    assert inbox.next_pending(route.session)["input_text"] == "persist me"
    unrelated = copy.deepcopy(frame)
    unrelated["d"]["channel_id"] = "999"
    assert reopened(unrelated) == []
