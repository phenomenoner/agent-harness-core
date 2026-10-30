from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.telegram_hold import hold_updates, inspect_held_updates
from ahc.infrastructure.telegram_inbox import TelegramInbox


def update(number, text="private input"):
    return {"update_id": number, "message": {"message_id": number + 1,
            "chat": {"id": 456}, "from": {"id": 789, "is_bot": False}, "text": text}}


def consumer_for(repo, snapshot):
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    return TelegramInbox(repo, [route])


def test_hold_and_cursor_commit_together_and_survive_reopen(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    consumer = consumer_for(repo, snapshot)

    result = hold_updates(consumer, [update(1), update(2)], expected_offset=None)

    assert result == {"held_count": 2, "update_ids": [1, 2], "next_offset": 3}
    assert "private input" not in str(result)
    assert consumer.offset() == 3
    assert RuntimeInbox(repo).next_pending(snapshot.session) is None
    reopened = consumer_for(RuntimeRepository(repo.root), snapshot)
    assert reopened.offset() == 3
    assert inspect_held_updates(reopened) == {"held_count": 2, "update_ids": [1, 2]}


@pytest.mark.parametrize("updates", [
    [update(1), {"update_id": "malformed"}],
    [update(1), update(1)],
])
def test_invalid_second_update_rolls_back_cursor_and_holds(tmp_path, updates):
    repo, snapshot, _ = setup(tmp_path)
    consumer = consumer_for(repo, snapshot)

    with pytest.raises(ContractError, match="Telegram update sequence invalid"):
        hold_updates(consumer, updates, expected_offset=None)

    assert consumer.offset() is None
    assert inspect_held_updates(consumer) == {"held_count": 0, "update_ids": []}
    assert RuntimeInbox(repo).next_pending(snapshot.session) is None


def test_wrong_offset_refuses_replay_and_preserves_existing_holds(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    consumer = consumer_for(repo, snapshot)
    hold_updates(consumer, [update(1)], expected_offset=None)

    with pytest.raises(ContractError, match="Telegram cursor changed"):
        hold_updates(consumer, [update(1)], expected_offset=None)

    assert consumer.offset() == 2
    assert inspect_held_updates(consumer) == {"held_count": 1, "update_ids": [1]}
    assert RuntimeInbox(repo).next_pending(snapshot.session) is None


def test_changed_route_or_session_is_rejected(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    consumer = consumer_for(repo, snapshot)
    route = consumer.routes[0]

    successor_route = TelegramInbox(repo, [replace(route, session=route.session.successor())])
    with pytest.raises(ContractError, match="Telegram route is not current and active"):
        hold_updates(successor_route, [update(1)], expected_offset=None)

    hold_updates(consumer, [update(1)], expected_offset=None)
    changed_route = TelegramInbox(repo, [replace(route, user_id="998")])
    with pytest.raises(ContractError, match="Telegram consumer route binding changed"):
        hold_updates(changed_route, [update(2)], expected_offset=2)
