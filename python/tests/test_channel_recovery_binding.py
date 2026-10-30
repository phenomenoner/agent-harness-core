import json
from dataclasses import replace

import pytest
from test_runtime_core import setup
from test_telegram_inbox import update

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_recovery_binding import rebind_recovered_consumer
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.telegram_inbox import TelegramInbox
from ahc.infrastructure.working_repository import WorkingWriterBusy


def consumer(repo, route):
    return (TelegramInbox(repo, [route]) if route.platform == "telegram"
            else DiscordResume(DiscordInbox(repo, [route])))


def position(value):
    return value.offset() if isinstance(value, TelegramInbox) else value.read()


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_explicit_rebind_preserves_position_and_old_pending(tmp_path, platform):
    repo, saved, _ = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", saved.session)
    old = consumer(repo, route)
    if platform == "telegram":
        old.admit([update(1)], expected_offset=None)
    else:
        old.save_ready("gateway-session", "wss://gateway.discord.gg", 1, expected=None)
        old.admit({"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": {
            "id": "10", "channel_id": "456", "author": {"id": "789"},
            "type": 0, "content": "old"}}, expected=old.read())
    checkpoint = position(old)
    with repo._read() as connection:
        pending = [tuple(row) for row in connection.execute("SELECT * FROM runtime_submissions")]
    backup, prep, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = backup_runtime_database(repo, backup)
    source_digest = _digest(repo.db_path)
    prepare_runtime_restore(backup, prep)
    receipt = transform_runtime_restore(prep, manifest["sha256"], (saved,),
                                        source_digest, transformed)
    lock = tmp_path / "supervisor.lock"
    activate_runtime_restore(repo, lock, transformed, receipt["sha256"],
                             manifest["sha256"], source_digest)
    recovered = repo.read_current("session").session
    successor = replace(route, session=recovered)
    new = consumer(repo, successor)
    with pytest.raises(ContractError, match="binding changed"):
        position(new)
    with pytest.raises(ContractError, match="checkpoint changed"):
        rebind_recovered_consumer(repo, lock, [route], [successor], expected=None)
    with pytest.raises(ContractError, match="external routes"):
        rebind_recovered_consumer(repo, lock, [route], [replace(successor, user_id="999")],
                                  expected=checkpoint)
    with repo.foreground_lease(recovered):
        with pytest.raises(WorkingWriterBusy):
            rebind_recovered_consumer(repo, lock, [route], [successor], expected=checkpoint)
    assert position(old) == checkpoint
    if platform == "discord":
        from test_foreground_config_v2 import valid_config
        from test_runtime_restore_cli import cli

        from ahc.cli.foreground_config_v2 import parse_foreground_config_v2

        paths, digests = [], []
        for index, bound in enumerate((route, successor)):
            config = valid_config()
            config["runtime_root"] = str(repo.root)
            config["session"] = bound.session.to_dict()
            config["channels"] = [{"route": bound.to_dict(), "token_env": "UNREAD_TOKEN",
                                   "enabled": False}]
            path = tmp_path / f"config-{index}.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            paths.append(path)
            digests.append(parse_foreground_config_v2(config).configuration_digest)
        arguments = ("runtime-channel-rebind", "--runtime-root", repo.root,
            "--supervisor-lock", lock, "--original-config", paths[0],
            "--recovered-config", paths[1], "--expected-original-digest", digests[0],
            "--expected-recovered-digest", digests[1], "--platform", platform,
            "--account-id", route.account_id, "--expected-checkpoint", json.dumps(checkpoint))
        cli(*arguments, "--expected-recovered-digest", "f" * 64, code=2)
        assert position(old) == checkpoint
        result = cli(*arguments)["restore"]
    else:
        result = rebind_recovered_consumer(repo, lock, [route], [successor], expected=checkpoint)
    assert result["checkpoint"] == checkpoint and not result["started"]
    assert position(new) == checkpoint
    with repo._read() as connection:
        assert [tuple(row) for row in connection.execute(
            "SELECT * FROM runtime_submissions")] == pending
    assert RuntimeInbox(repo).next_pending(recovered) is None
    with pytest.raises(ContractError, match="binding changed"):
        position(old)
    with pytest.raises(ContractError, match="binding changed"):
        rebind_recovered_consumer(repo, lock, [route], [successor], expected=checkpoint)


def test_ordinary_generation_cannot_rebind_consumer(tmp_path):
    from ahc.application.working_continuity import WorkingContinuityService

    repo, saved, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", saved.session)
    old = TelegramInbox(repo, [route])
    old.admit([update(1)], expected_offset=None)
    continued = WorkingContinuityService(repo).continue_generation(
        saved, saved.session.successor(), "compaction")
    with pytest.raises(ContractError, match="lineage"):
        rebind_recovered_consumer(repo, tmp_path / "supervisor.lock", [route],
                                  [replace(route, session=continued.session)], expected=2)
    assert old.offset() == 2
