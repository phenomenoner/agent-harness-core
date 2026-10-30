"""The shared continuation preserves Discord transport position and core state."""
import json
import sys
from dataclasses import replace

import pytest
from skill_test_support import repository, skill_root
from test_channel_skill_adoption import active
from test_runtime_core import setup

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.channel_continuation import (
    continue_discord_session,
    read_skill_continuation_receipt,
)
from ahc.infrastructure.channel_schedule_routes import ChannelScheduleRoutes
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingWriterBusy

__all__ = ["repository", "skill_root"]


def configured(tmp_path, *, checkpoint=True):
    store, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    resume = DiscordResume(DiscordInbox(store, [route]))
    if checkpoint:
        resume.save_ready("gateway-session", "wss://gateway.discord.gg", 2, expected=None)
    return store, snapshot, route, resume, tmp_path / "supervisor.lock"


def transition(store, lock, old, route, expected_resume, **kwargs):
    return continue_discord_session(store, lock, old, [route],
                                    expected_resume=expected_resume, **kwargs)


def test_discord_keeps_position_preferences_schedules_and_new_event_admission(tmp_path):
    store, old, route, resume, lock = configured(tmp_path)
    checkpoint = resume.read()
    schedule = IntervalSchedule("retained", old.session, old.goal.digest, 1000, 100, "next")
    ScheduleRepository(store).create(schedule)
    payload = canonical_json({"model": "chosen", "effort": "max", "revision": 9}).decode()

    def seed(db):
        ChannelScheduleRoutes.bind(db, schedule, route)
        db.execute("CREATE TABLE channel_preferences (route_json TEXT PRIMARY KEY,payload_json TEXT NOT NULL)")
        db.execute("INSERT INTO channel_preferences VALUES(?,?)",
                   (canonical_json(route.to_dict()).decode(), payload))
    store._mutate(seed)
    first = transition(store, lock, old, route, checkpoint)
    assert first["state"] == "applied" and first["resume"] == checkpoint
    assert transition(store, lock, old, route, checkpoint)["state"] == "already-applied"
    new = store.read_current("session")
    current_route = replace(route, session=new.session)
    current_resume = DiscordResume(DiscordInbox(store, [current_route]))
    assert current_resume.read() == checkpoint
    with pytest.raises(ContractError, match="route binding changed"):
        resume.read()
    moved = ScheduleRepository(store).read(new.session, schedule.schedule_id)
    assert moved == replace(schedule, session=new.session, revision=2)
    with store._read() as db:
        assert ChannelScheduleRoutes.route_for(db, moved) == current_route
        values = [r[0] for r in db.execute("SELECT payload_json FROM channel_preferences")]
        assert values == [payload, payload]
    assert RuntimeInbox(store).next_pending(new.session) is None
    frame = {"op": 0, "s": 3, "t": "MESSAGE_CREATE", "d": {
        "id": "10", "channel_id": "456", "author": {"id": "789"},
        "type": 0, "content": "new generation"}}
    assert len(current_resume.admit(frame, expected=checkpoint)) == 1
    assert RuntimeInbox(store).next_pending(new.session)["input_text"] == "new generation"
    with pytest.raises(ContractError):
        current_resume.admit(frame, expected=checkpoint)
    assert store.read_current("session").session.ref.generation == 2


def test_discord_precommit_failure_keeps_old_state_and_inert_target_pin(tmp_path, repository):
    store, old, route, resume, lock = configured(tmp_path)
    checkpoint = resume.read()
    revision = active(repository, "chosen", "discord-choice")
    schedule = IntervalSchedule("retained", old.session, old.goal.digest, 1000, 100, "next")
    ScheduleRepository(store).create(schedule)
    store._mutate(lambda db: ChannelScheduleRoutes.bind(db, schedule, route))
    target = dict(skill_repository=repository, expected_skill_revisions=(revision,))
    with pytest.raises(RuntimeError, match="precommit"):
        transition(store, lock, old, route, checkpoint, fail_before_commit=True, **target)
    assert store.read_current("session") == old and resume.read() == checkpoint
    assert ScheduleRepository(store).read(old.session, "retained") == schedule
    assert read_skill_continuation_receipt(store, old) is None
    pinned, existed = repository.pin_revision_availability(old.session.successor())
    assert existed and [r.revision_digest for r in pinned] == [revision]
    assert transition(store, lock, old, route, checkpoint, **target)["state"] == "applied"
    with pytest.raises(ContractError, match="target must be retained"):
        transition(store, lock, old, route, checkpoint)


@pytest.mark.parametrize("change", ["account", "channel", "user", "session", "sequence",
                                   "gateway-session", "url", "malformed"])
def test_discord_unproven_binding_rejects_without_mutation(tmp_path, change):
    store, old, route, resume, lock = configured(tmp_path)
    checkpoint = resume.read()
    altered, position = route, dict(checkpoint)
    if change in ("account", "channel", "user"):
        altered = replace(route, **{change + "_id": "999"})
    elif change == "session":
        altered = replace(route, session=old.session.successor())
    elif change == "sequence":
        position["sequence"] += 1
    elif change == "gateway-session":
        position["session_id"] = "other-session"
    elif change == "url":
        position["resume_url"] = "wss://evil.example"
    else:
        position["extra"] = True
    with pytest.raises((ContractError, WorkingSetConflict)):
        transition(store, lock, old, altered, position)
    assert store.read_current("session") == old and resume.read() == checkpoint


def test_discord_without_resume_still_binds_retry_routes(tmp_path):
    store, old, route, resume, lock = configured(tmp_path, checkpoint=False)
    assert transition(store, lock, old, route, None)["state"] == "applied"
    assert transition(store, lock, old, route, None)["state"] == "already-applied"
    with pytest.raises(ContractError, match="consumer binding changed"):
        transition(store, lock, old, replace(route, channel_id="999"), None)
    assert read_skill_continuation_receipt(store, old)["consumer"]["resume"] is None
    assert store.read_current("session").session.ref.generation == 2


def test_discord_retry_rejects_changed_transport_checkpoint(tmp_path):
    store, old, route, resume, lock = configured(tmp_path)
    checkpoint = resume.read()
    transition(store, lock, old, route, checkpoint)
    new = store.read_current("session")
    successor_resume = DiscordResume(DiscordInbox(store, [replace(route, session=new.session)]))
    successor_resume.save_ready(checkpoint["session_id"], checkpoint["resume_url"], 3,
                                expected=checkpoint)
    with pytest.raises(WorkingSetConflict, match="cursor changed"):
        transition(store, lock, old, route, checkpoint)
    assert store.read_current("session") == new


def test_discord_live_lease_and_undispatched_work_block_continuation(tmp_path):
    store, old, route, resume, lock = configured(tmp_path)
    with store.foreground_lease(old.session):
        with pytest.raises(WorkingWriterBusy):
            transition(store, lock, old, route, resume.read())
    RuntimeInbox(store).submit(old.session, "pending", old.goal.digest, "input")
    with pytest.raises(ContractError, match="undispatched"):
        transition(store, lock, old, route, resume.read())
    assert store.read_current("session") == old


@pytest.mark.skipif(sys.platform != "linux", reason="native foreground configuration")
def test_discord_cli_emits_same_core_successor_without_network(tmp_path, capsys):
    from ahc.cli.channel_continuation import main
    from ahc.cli.foreground_config_native import parse_trusted_foreground_config
    from test_foreground_config_native import native_config

    store, old, route, resume, lock = configured(tmp_path)
    value = native_config()
    value.update(runtime_root=str(store.root), session=old.session.to_dict())
    value.pop("telegram_polling", None)
    value["channels"] = [{"route": route.to_dict(), "token_env": "UNUSED", "enabled": False}]
    value["discord_gateway"] = {"enabled": False, "account_id": "123", "token_env": "UNUSED",
                                "intents": 0, "connection_limit": 2}
    config = parse_trusted_foreground_config(value)
    paths = {name: tmp_path / (name + ".json") for name in ("config", "snapshot", "resume", "output")}
    for name, document in (("config", value), ("snapshot", old.to_dict()), ("resume", resume.read())):
        paths[name].write_text(json.dumps(document))
    assert main(["--config", str(paths["config"]), "--expected-config-digest",
                 config.configuration_digest, "--expected-snapshot", str(paths["snapshot"]),
                 "--expected-discord-resume", str(paths["resume"]), "--supervisor-lock", str(lock),
                 "--output-config", str(paths["output"])]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "applied" and result["resume"] == json.loads(paths["resume"].read_bytes())
    emitted = json.loads(paths["output"].read_bytes())
    assert emitted["session"] == old.session.successor().to_dict()
    assert emitted["channels"][0]["route"]["platform"] == "discord"
