from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.telegram_inbox import TelegramInbox
from ahc.infrastructure.telegram_hold import inspect_held_updates


def update(number, text="hello", user=789):
    return {"update_id": number, "message": {"message_id": number + 1,
            "chat": {"id": 456}, "from": {"id": user, "is_bot": False}, "text": text}}


def test_cursor_and_events_commit_together_and_survive_reopen(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    consumer = TelegramInbox(repo, [route])
    assert consumer.offset() is None
    with pytest.raises(ContractError):
        consumer.admit([update(1), update(1)], expected_offset=None)
    assert consumer.offset() is None
    assert RuntimeInbox(repo).next_pending(route.session) is None
    result = consumer.admit([update(1), update(2, user=999)], expected_offset=None)
    assert result["next_offset"] == 3 and len(result["submissions"]) == 1
    reopened = TelegramInbox(RuntimeRepository(repo.root), [route])
    assert reopened.offset() == 3
    with pytest.raises(ContractError, match="cursor changed"):
        reopened.admit([update(1)], expected_offset=None)
    assert reopened.offset() == 3
    with pytest.raises(ContractError, match="binding changed"):
        TelegramInbox(repo, [replace(route, user_id="999")]).offset()
    assert RuntimeInbox(repo).next_pending(route.session)["input_text"] == "hello"


@pytest.mark.parametrize("malformed", [
    {"message": None}, {"message": {"from": []}},
    {"message": {"from": {"id": 789}, "chat": None}},
    {"message": {"from": {"id": 789}, "chat": {"id": 456}}},
    {"message": {"from": {"id": 789, "is_bot": False}, "chat": {"id": 456}}},
    update(1, text=123), {**update(1, text=None), "message": {
        **update(1, text=None)["message"], "caption": []}},
])
def test_invalid_message_is_retained_before_cursor_and_next_input_survives(tmp_path, malformed):
    import json

    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    consumer = TelegramInbox(repo, [route])
    bad = {**malformed, "update_id": 1}
    result = consumer.admit([bad, update(2)], expected_offset=None)
    assert result["held_count"] == 1 and len(result["submissions"]) == 1
    assert consumer.offset() == 3
    reopened = TelegramInbox(RuntimeRepository(repo.root), [route])
    assert inspect_held_updates(reopened) == {"held_count": 1, "update_ids": [1]}
    with repo._read() as connection:
        row = connection.execute("SELECT * FROM runtime_telegram_holds").fetchone()
        assert json.loads(row["update_json"]) == bad
        assert row["account_id"] == "123" and row["routes_json"] == consumer._routes_json
    assert RuntimeInbox(repo).next_pending(route.session)["input_text"] == "hello"
    with pytest.raises(ContractError, match="cursor changed"):
        reopened.admit([bad, update(2)], expected_offset=None)
    assert inspect_held_updates(reopened)["held_count"] == 1


def test_hold_failure_and_later_cursor_failure_roll_back_the_entire_batch(tmp_path):
    from ahc.infrastructure.working_repository import WorkingRepositoryError
    from ahc.infrastructure.telegram_hold import _TABLE as holds_table
    from ahc.infrastructure.telegram_inbox import _TABLE as cursor_table

    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    consumer = TelegramInbox(repo, [route])
    for table in ("runtime_telegram_holds", "runtime_telegram_cursors"):
        def install(connection):
            connection.execute(holds_table)
            connection.execute(cursor_table)
            connection.execute(f"CREATE TRIGGER reject_write BEFORE INSERT ON {table} "
                               "BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END")
        repo._mutate(install)
        with pytest.raises(WorkingRepositoryError) as error:
            consumer.admit([update(1, text=123), update(2)], expected_offset=None)
        assert "injected storage failure" in str(error.value.__cause__)
        assert consumer.offset() is None
        assert inspect_held_updates(consumer)["held_count"] == 0
        assert RuntimeInbox(repo).next_pending(route.session) is None
        repo._mutate(lambda connection: connection.execute("DROP TRIGGER reject_write"))


def test_foreign_text_and_stale_local_route_cannot_become_held_input(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    consumer = TelegramInbox(repo, [route])
    result = consumer.admit([update(1, text=123, user=999)], expected_offset=None)
    assert result.get("held_count", 0) == 0 and not result["submissions"]
    stale = TelegramInbox(repo, [replace(route, session=route.session.successor())])
    with pytest.raises(ContractError):
        stale.admit([update(2, text=123)], expected_offset=2)
    assert consumer.offset() == 2 and inspect_held_updates(consumer)["held_count"] == 0
