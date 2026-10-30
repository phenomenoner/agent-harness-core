"""Controlled album ingress, durable restart and continuation contracts."""

from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.telegram_inbox import TelegramInbox
from test_runtime_core import setup


def photo(number, *, group="album-a", caption=None, chat=456, reply=None):
    message = {
        "message_id": number + 100,
        "chat": {"id": chat},
        "from": {"id": 789, "is_bot": False},
        "media_group_id": group,
        "photo": [{"file_id": "file_" + str(number), "file_unique_id": "u" + str(number),
                   "width": 2, "height": 2, "file_size": 10}],
    }
    if caption is not None:
        message["caption"] = caption
    if reply is not None:
        message["reply_to_message"] = reply
    return {"update_id": number, "message": message}


def consumer_for(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    from types import SimpleNamespace
    from ahc.domain.runtime_policy import EffectivePolicy
    from ahc.infrastructure.channel_control_store import ChannelControlStore

    document = {"channels": [{"route": route.to_dict(), "enabled": True}],
                "channel_control": {"enabled": True, "models": {
                    "fixture": {"efforts": ["max"], "priority": False}}, "aliases": {}}}
    config = SimpleNamespace(to_dict=lambda: document, session=snapshot.session,
                             effective_policy=EffectivePolicy(
                                 1, "manual", "workspace-write", True, "a" * 64, 30),
                             backend={"model": "fixture", "reasoning_effort": "max"})
    repo.channel_controls = ChannelControlStore(repo, config)
    clock = [0]
    consumer = TelegramInbox(repo, [route], include_files=True)
    consumer._album_clock = lambda: clock[0]
    return repo, route, consumer, clock


def test_album_waits_for_quiet_then_stages_one_ordered_task(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    result = consumer.admit([photo(1, caption="Compare these"), photo(2)], expected_offset=None)
    assert result["next_offset"] == 3
    assert ChannelFileInbox(repo).pending(route) == []
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    staged = ChannelFileInbox(repo).pending(route)
    assert len(staged) == 1
    event, _ = staged[0]
    assert [item.file_id for item in event.files] == ["file_1", "file_2"]
    assert event.input_text == "Compare these"
    assert event.context["message_id"] == "101"
    assert event.context["album_members"] == [
        {"message_id": "101", "attachment_indexes": [0]},
        {"message_id": "102", "attachment_indexes": [1]},
    ]
    assert consumer.flush_albums() == []
    assert len(ChannelFileInbox(RuntimeRepository(repo.root)).pending(route)) == 1


def test_split_poll_album_survives_reopen_and_restarts_quiet_window(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    consumer.admit([photo(1, caption="Compare")], expected_offset=None)
    reopened = TelegramInbox(RuntimeRepository(repo.root), [route], include_files=True)
    reopened._album_clock = lambda: clock[0]
    clock[0] = 2_000_000_000
    reopened.admit([photo(2)], expected_offset=2)
    assert ChannelFileInbox(repo).pending(route) == []
    clock[0] = 2_999_999_999
    assert reopened.flush_albums() == []
    clock[0] = 3_000_000_000
    assert len(reopened.flush_albums()) == 1
    assert len(ChannelFileInbox(repo).pending(route)[0][0].files) == 2
    assert reopened.offset() == 3


def test_same_group_id_never_merges_channels(tmp_path):
    from dataclasses import replace

    repo, route, _, clock = consumer_for(tmp_path)
    other = replace(route, channel_id="457")
    consumer = TelegramInbox(repo, [route, other], include_files=True)
    consumer._album_clock = lambda: clock[0]
    consumer.admit([photo(1), photo(2, chat=457)], expected_offset=None)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    left = ChannelFileInbox(repo).pending(route)[0][0]
    right = ChannelFileInbox(repo).pending(other)[0][0]
    assert left.operation_id != right.operation_id
    assert [item.file_id for item in left.files] == ["file_1"]
    assert [item.file_id for item in right.files] == ["file_2"]


def test_late_member_cannot_rewrite_an_admitted_album(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    consumer.admit([photo(1)], expected_offset=None)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    original = ChannelFileInbox(repo).pending(route)[0][0]
    consumer.admit([photo(2)], expected_offset=2)
    staged = ChannelFileInbox(repo).pending(route)
    assert staged[0][0] == original
    assert len(staged) == 2
    assert staged[1][0].operation_id != original.operation_id


def test_over_limit_album_is_explicit_notice_and_imports_no_partial_files(tmp_path):
    from ahc.infrastructure.runtime_inbox import RuntimeInbox

    repo, route, consumer, clock = consumer_for(tmp_path)
    consumer.admit([photo(number) for number in range(1, 10)], expected_offset=None)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    assert ChannelFileInbox(repo).pending(route) == []
    assert RuntimeInbox(repo).next_pending(route.session) is None
    with repo._read() as connection:
        rows = connection.execute("SELECT context_json FROM runtime_channel_context").fetchall()
    import json

    context = json.loads(rows[-1]["context_json"])
    assert "eight-attachment limit" in context["notice"]
    assert len(context["album_members"]) == 9
    assert all(member["attachment_indexes"] == [] for member in context["album_members"])


def test_member_reply_identity_is_preserved_and_reference_file_deduplicated(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    reference = {"message_id": 90, "chat": {"id": 456}, "from": {"id": 789},
                 "caption": "Earlier image", "photo": [
                     {"file_id": "reference", "file_unique_id": "reference-u",
                      "width": 2, "height": 2, "file_size": 10}]}
    consumer.admit([photo(1, caption="Compare", reply=reference),
                    photo(2, reply=reference)], expected_offset=None)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    event = ChannelFileInbox(repo).pending(route)[0][0]
    assert [item.file_id for item in event.files] == ["file_1", "reference", "file_2"]
    assert event.context["reply_to_id"] == "90"
    assert event.context["album_members"] == [
        {"message_id": "101", "attachment_indexes": [0, 1], "reply_to_id": "90"},
        {"message_id": "102", "attachment_indexes": [2, 1], "reply_to_id": "90"},
    ]
    assert "reference" not in str(event.context["album_members"])


def test_rollback_keeps_cursor_and_album_members_atomic(tmp_path):
    import pytest
    from ahc.domain.sessions import ContractError

    repo, route, consumer, clock = consumer_for(tmp_path)
    with pytest.raises(ContractError):
        consumer.admit([photo(1), photo(1)], expected_offset=None)
    assert consumer.offset() is None
    clock[0] = 2_000_000_000
    assert consumer.flush_albums() == []
    assert ChannelFileInbox(repo).pending(route) == []


def test_clock_rollback_cannot_strand_a_retained_album(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    clock[0] = 5_000_000_000
    consumer.admit([photo(1)], expected_offset=None)
    clock[0] = 1
    assert len(consumer.flush_albums()) == 1
    assert len(ChannelFileInbox(repo).pending(route)) == 1


def test_combined_caption_bound_is_visible_without_partial_import(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    consumer.admit([photo(1, caption="a" * 3000), photo(2, caption="b" * 3000)],
                   expected_offset=None)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    assert ChannelFileInbox(repo).pending(route) == []
    with repo._read() as connection:
        row = connection.execute("SELECT context_json FROM runtime_channel_context").fetchone()
    import json

    context = json.loads(row["context_json"])
    assert "captions exceed 4096" in context["notice"]


def test_album_import_and_reopen_submit_one_task_with_both_artifacts(tmp_path):
    from ahc.infrastructure.artifact_repository import ArtifactRepository
    from ahc.infrastructure.channel_file_import import ChannelFileImport
    from ahc.infrastructure.runtime_inbox import RuntimeInbox
    from test_artifacts import state

    repo, snapshot, producer, _ = state(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    consumer = TelegramInbox(repo, [route], include_files=True)
    clock = [0]
    consumer._album_clock = lambda: clock[0]
    updates = []
    for number, filename in [(1, "one.txt"), (2, "two.txt")]:
        item = photo(number, caption="Read both" if number == 1 else None)
        del item["message"]["photo"]
        item["message"]["document"] = {
            "file_id": "doc_" + str(number), "file_unique_id": "d" + str(number),
            "file_name": filename, "file_size": 4, "mime_type": "text/plain",
        }
        updates.append(item)
    consumer.admit(updates, expected_offset=None)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    event, goal = ChannelFileInbox(repo).pending(route)[0]
    calls = []

    class Download:
        def fetch(self, reference):
            calls.append(reference.file_id)
            return b"one!" if reference.filename == "one.txt" else b"two!"

    importer = ChannelFileImport(repo)
    selected = importer.run(event, goal, producer, Download())
    reopened = RuntimeRepository(repo.root)
    assert ChannelFileImport(reopened).run(event, goal, producer, Download()) == selected
    assert calls == ["doc_1", "doc_2"]
    for item, expected in zip(selected, [b"one!", b"two!"], strict=True):
        assert ArtifactRepository(reopened).read(route.session, item["artifact_id"])[1] == expected
    submitter = ChannelFileImport(reopened)
    receipt = submitter.submit(event, goal)
    assert submitter.submit(event, goal) == receipt
    pending = RuntimeInbox(reopened).next_pending(route.session)
    assert pending["operation_id"] == event.operation_id
    assert pending["input_text"] == "Read both"
    from ahc.infrastructure.channel_reply_catalog import known_messages, validate_reply
    with reopened._read() as connection:
        known = known_messages(connection, route.session, event.operation_id)
        assert {"101", "102"} <= {message["message_id"] for message in known}
        validate_reply(connection, route.session, event.operation_id, "102")
    with reopened._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1


def test_new_holds_buffered_and_late_album_members_without_using_pending_capacity(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    consumer.admit([photo(n, group="old-" + str(n)) for n in range(1, 65)],
                   expected_offset=None)
    reset = {"update_id": 65, "message": {"message_id": 165,
             "chat": {"id": 456}, "from": {"id": 789, "is_bot": False}, "text": "/new"}}
    consumer.admit([reset], expected_offset=65)
    consumer.admit([photo(66, group="old-1"), photo(67, group="new-group")],
                   expected_offset=66)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    pending = ChannelFileInbox(repo).pending(route)
    assert len(pending) == 1
    assert pending[0][0].context["media_group_id"] == "new-group"
    with repo._read() as connection:
        assert connection.execute("SELECT count(*) FROM channel_held").fetchone()[0] == 64
        held = connection.execute(
            "SELECT state,events_json FROM runtime_telegram_albums WHERE group_id='old-1'"
        ).fetchone()
    import json

    assert held["state"] == "held"
    assert len(json.loads(held["events_json"])) == 2


def test_long_reference_context_keeps_supported_files_executable(tmp_path):
    from ahc.infrastructure.channel_file_import import ChannelFileImport
    from ahc.infrastructure.runtime_inbox import RuntimeInbox
    from ahc.domain.skill_values import canonical_json

    repo, route, consumer, clock = consumer_for(tmp_path)
    updates = []
    for number in range(1, 9):
        item = photo(number, caption="a" * 3000, reply={
            "message_id": 90, "chat": {"id": 456}, "text": "q" * 3000})
        del item["message"]["photo"]
        item["message"]["document"] = {
            "file_id": "doc_" + str(number), "file_unique_id": "u" + str(number),
            "file_name": str(number) + "z" * 123 + ".txt",
            "mime_type": "text/plain", "file_size": 4,
        }
        item["message"]["forward_origin"] = {
            "type": "hidden_user", "sender_user_name": "fixture", "date": 1}
        updates.append(item)
    consumer.admit(updates, expected_offset=None)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    event, goal = ChannelFileInbox(repo).pending(route)[0]
    assert len(event.files) == 8 and "notice" not in event.context
    assert event.context["quote_status"] == "truncated"
    assert len(canonical_json(event.context).decode()) <= 8000
    producer = repo.reserve_workspace_generation(repo.read_current("session"), "long-reference")

    class Download:
        def fetch(self, reference):
            return b"file"

    importer = ChannelFileImport(repo)
    importer.run(event, goal, producer, Download())
    importer.submit(event, goal)
    assert RuntimeInbox(repo).next_pending(route.session) is not None
    assert ChannelFileInbox(repo).pending(route) == []


def test_optional_bad_reply_id_and_large_accepted_message_id_cannot_poison_polling(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    item = photo(1, reply={"message_id": "bad-id", "chat": {"id": 456}, "text": "reference"})
    item["message"]["message_id"] = 2**63
    consumer.admit([item], expected_offset=None)
    assert consumer.offset() == 2
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    assert len(ChannelFileInbox(repo).pending(route)) == 1
    first = ChannelFileInbox(repo).pending(route)[0][0]
    assert first.context["album_members"] == [
        {"message_id": str(2**63), "attachment_indexes": [0]}]
    consumer.admit([photo(2, group="another")], expected_offset=2)
    assert consumer.offset() == 3
    clock[0] = 2_000_000_000
    consumer.flush_albums()
    assert len(ChannelFileInbox(repo).pending(route)) == 2


def test_unrepresentable_remote_member_id_is_quarantined_without_pinning_cursor(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    invalid = photo(1)
    invalid["message"]["message_id"] = int("9" * 129)
    receipt = consumer.admit([invalid], expected_offset=None)
    assert receipt["held_count"] == 1 and consumer.offset() == 2
    assert ChannelFileInbox(repo).pending(route) == []
    consumer.admit([photo(2, group="fresh")], expected_offset=2)
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    assert consumer.offset() == 3
    pending = ChannelFileInbox(repo).pending(route)
    assert len(pending) == 1 and pending[0][0].files[0].file_id == "file_2"


def test_duplicate_remote_member_id_is_quarantined_without_poisoning_the_album(tmp_path):
    repo, route, consumer, clock = consumer_for(tmp_path)
    consumer.admit([photo(1)], expected_offset=None)
    duplicate = photo(2)
    duplicate["message"]["message_id"] = 101
    receipt = consumer.admit([duplicate], expected_offset=2)
    assert receipt["held_count"] == 1 and consumer.offset() == 3
    clock[0] = 1_000_000_000
    consumer.flush_albums()
    pending = ChannelFileInbox(repo).pending(route)
    assert len(pending) == 1
    assert [item.file_id for item in pending[0][0].files] == ["file_1"]
    consumer.admit([photo(3, group="fresh")], expected_offset=3)
    clock[0] = 2_000_000_000
    consumer.flush_albums()
    assert consumer.offset() == 4 and len(ChannelFileInbox(repo).pending(route)) == 2


def test_local_album_corruption_is_not_misclassified_as_remote_quarantine(tmp_path):
    import pytest
    from ahc.infrastructure.working_repository import WorkingSetCorrupt

    repo, route, consumer, _ = consumer_for(tmp_path)
    consumer.admit([photo(1)], expected_offset=None)
    repo._mutate(lambda connection: connection.execute(
        "UPDATE runtime_telegram_albums SET events_json='not json'"))
    with pytest.raises(WorkingSetCorrupt):
        consumer.admit([photo(2)], expected_offset=2)
    assert consumer.offset() == 2
