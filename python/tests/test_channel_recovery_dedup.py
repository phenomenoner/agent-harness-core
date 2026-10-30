from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.adapters.channel_events import decode_channel_event
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.channel_recovery_binding import rebind_recovered_consumer
from ahc.infrastructure.channel_recovery_dedup import retained_recovery_event
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore


def recover(repo, saved, tmp_path):
    manifest = backup_runtime_database(repo, tmp_path / "backup")
    digest = _digest(repo.db_path)
    prepare_runtime_restore(tmp_path / "backup", tmp_path / "prep")
    receipt = transform_runtime_restore(tmp_path / "prep", manifest["sha256"], (saved,),
                                        digest, tmp_path / "new")
    activate_runtime_restore(repo, tmp_path / "supervisor.lock", tmp_path / "new",
                             receipt["sha256"], manifest["sha256"], digest)
    return repo.read_current("session").session


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("files", [False, True])
def test_retained_event_requires_exact_content_and_recovery(tmp_path, platform, files):
    repo, saved, _ = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", saved.session)
    url = ("https://cdn.discordapp.com/attachments/456/2/report.txt"
           if platform == "discord" else None)
    attachments = ((ChannelFile(platform, "456", "2", "report.txt", 4, "text/plain", url),)
                   if files else ())
    event = ChannelInboundEvent(route, "event-1", "read", attachments)
    if files:
        ChannelFileInbox(repo).stage(event, saved.goal.digest)
    else:
        RuntimeInbox(repo).submit(saved.session, event.operation_id, saved.goal.digest,
                                 event.input_text, channel_event=event)
    successor = recover(repo, saved, tmp_path)
    projected = replace(event, route=replace(route, session=successor))
    with repo._read() as connection:
        result = retained_recovery_event(repo, connection, projected)
        assert result["status"] == "historical-event-retained" and not result["submitted"]
        with pytest.raises(ContractError, match="conflict"):
            retained_recovery_event(repo, connection, replace(projected, input_text="changed"))
        if files:
            with pytest.raises(ContractError, match="conflict"):
                retained_recovery_event(repo, connection, replace(projected, files=()))
        assert retained_recovery_event(repo, connection, replace(projected, event_id="new")) is None
    assert RuntimeInbox(repo).next_pending(successor) is None


def test_discord_duplicate_advances_checkpoint_without_replaying_old_pending(tmp_path):
    repo, saved, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", saved.session)
    old = DiscordResume(DiscordInbox(repo, [route]))
    old.save_ready("gateway", "wss://gateway.discord.gg", 1, expected=None)
    frame = {"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": {"id": "10",
        "channel_id": "456", "author": {"id": "789"}, "type": 0, "content": "old"}}
    old.admit(frame, expected=old.read())
    before = old.read()
    successor = recover(repo, saved, tmp_path)
    new_route = replace(route, session=successor)
    rebind_recovered_consumer(repo, tmp_path / "supervisor.lock", [route], [new_route],
                              expected=before)
    new = DiscordResume(DiscordInbox(repo, [new_route]))
    replay = {**frame, "s": 3}
    bad = {**replay, "d": {**frame["d"], "content": "changed"}}
    with pytest.raises(ContractError, match="conflict"):
        new.admit(bad, expected=before)
    assert new.read() == before
    result = new.admit(replay, expected=before)
    assert result[0]["status"] == "historical-event-retained"
    assert new.read()["sequence"] == 3
    assert RuntimeInbox(repo).next_pending(successor) is None
    fresh = {**frame, "s": 4, "d": {**frame["d"], "id": "11", "content": "new"}}
    new.admit(fresh, expected=new.read())
    assert RuntimeInbox(repo).next_pending(successor)["input_text"] == "new"
    original = decode_channel_event(route, frame)
    with repo._read() as connection:
        row = connection.execute("SELECT * FROM runtime_submissions WHERE operation_id=?",
                                  (original.operation_id,)).fetchone()
        assert RuntimeInbox._decode(row)["session"] == saved.session.to_dict()


def test_ordinary_continuation_does_not_acknowledge_ancestor_event(tmp_path):
    from ahc.application.working_continuity import WorkingContinuityService

    repo, saved, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", saved.session)
    event = ChannelInboundEvent(route, "message-10", "old")
    RuntimeInbox(repo).submit(saved.session, event.operation_id, saved.goal.digest,
                             event.input_text, channel_event=event)
    current = WorkingContinuityService(repo).continue_generation(
        saved, saved.session.successor(), "compaction")
    projected = replace(event, route=replace(route, session=current.session))
    with repo._read() as connection:
        with pytest.raises(ContractError, match="conflict"):
            retained_recovery_event(repo, connection, projected)
