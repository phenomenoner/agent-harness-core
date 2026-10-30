"""Real SQLite continuation boundaries; no provider or external channel calls."""

from dataclasses import replace

import pytest
from test_runtime_core import setup
from test_telegram_inbox import update

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.schedules import IntervalSchedule
from ahc.infrastructure.channel_continuation import continue_telegram_session
from ahc.infrastructure.channel_schedule_routes import ChannelScheduleRoutes
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_recovery_access import historical_session_allows, recovery_allows
from ahc.infrastructure.schedule_occurrences import ScheduleOccurrences
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.telegram_inbox import TelegramInbox
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingWriterBusy


def configured(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    consumer = TelegramInbox(store, [route])
    consumer.admit([update(1, user=999)], expected_offset=None)
    assert consumer.offset() == 2
    return store, snapshot, route, consumer, tmp_path / "supervisor.lock"


def transition(store, lock, snapshot, routes, **kwargs):
    return continue_telegram_session(store, lock, snapshot, routes,
                                    expected_offset=kwargs.pop("expected_offset", 2), **kwargs)


def test_atomic_continuation_preserves_cursor_schedule_and_read_ancestry(tmp_path):
    store, old, route, consumer, lock = configured(tmp_path)
    value = IntervalSchedule("s", old.session, old.goal.digest, 1000, 100, "input")
    ScheduleRepository(store).create(value)
    store._mutate(lambda c: ChannelScheduleRoutes.bind(c, value, route))
    result = transition(store, lock, old, [route])
    new = store.read_current("session")
    assert result["state"] == "applied" and result["migrated_schedules"] == 1
    assert new.session == old.session.successor() and new.goal == old.goal
    successor_route = replace(route, session=new.session)
    assert TelegramInbox(store, [successor_route]).offset() == 2
    with pytest.raises(ContractError, match="binding changed"):
        consumer.admit([update(2)], expected_offset=2)
    moved = ScheduleRepository(store).read(new.session, "s")
    assert moved == replace(value, session=new.session, revision=2)
    with store._read() as c:
        assert ChannelScheduleRoutes.route_for(c, moved) == successor_route
        assert historical_session_allows(store, c, new.session, old.session)
        assert not recovery_allows(store, c, new.session, old.session)
        assert not historical_session_allows(store, c, new.session, replace(old.session, agent_id="foreign"))
        assert not historical_session_allows(store, c, new.session, replace(old.session, route_id="foreign"))
    admitted = ScheduleOccurrences(store).enqueue(moved, 1000, 1000)
    assert admitted is not None
    assert RuntimeInbox(store).next_pending(new.session) is not None


def test_precommit_failure_rolls_back_snapshot_consumer_and_schedule(tmp_path):
    store, old, route, consumer, lock = configured(tmp_path)
    value = IntervalSchedule("s", old.session, old.goal.digest, 1000, 100, "input")
    ScheduleRepository(store).create(value)
    store._mutate(lambda c: ChannelScheduleRoutes.bind(c, value, route))
    with pytest.raises(RuntimeError, match="precommit"):
        transition(store, lock, old, [route], fail_before_commit=True)
    assert store.read_current("session") == old and consumer.offset() == 2
    assert ScheduleRepository(store).read(old.session, "s") == value
    with store._read() as c:
        assert c.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 1
        assert ChannelScheduleRoutes.route_for(c, value) == route


def test_postcommit_retry_reports_same_successor_without_advancing(tmp_path):
    store, old, route, consumer, lock = configured(tmp_path)
    first = transition(store, lock, old, [route])
    second = transition(store, lock, old, [route])
    assert first["working_set"] == second["working_set"]
    assert second["state"] == "already-applied"
    assert store.read_current("session").session.ref.generation == 2


@pytest.mark.parametrize("change", ["cursor", "route", "owner", "snapshot"])
def test_wrong_expected_authority_never_mutates(tmp_path, change):
    store, old, route, consumer, lock = configured(tmp_path)
    expected, routes, kwargs = old, [route], {}
    if change == "cursor": kwargs["expected_offset"] = 3
    if change == "route": routes = [replace(route, user_id="888")]
    if change == "owner": expected = replace(old, session=replace(old.session, agent_id="foreign"), snapshot_digest=None)
    if change == "snapshot": expected = old.revise()
    with pytest.raises((ContractError, WorkingSetConflict)):
        transition(store, lock, expected, routes, **kwargs)
    assert store.read_current("session") == old and consumer.offset() == 2


def test_undispatched_submission_blocks_transition(tmp_path):
    store, old, route, consumer, lock = configured(tmp_path)
    RuntimeInbox(store).submit(old.session, "pending", old.goal.digest, "input")
    with pytest.raises(ContractError, match="undispatched"):
        transition(store, lock, old, [route])
    assert store.read_current("session") == old and consumer.offset() == 2


def test_current_foreground_lease_blocks_offline_transition(tmp_path):
    store, old, route, consumer, lock = configured(tmp_path)
    with store.foreground_lease(old.session):
        with pytest.raises(WorkingWriterBusy):
            transition(store, lock, old, [route])
    assert store.read_current("session") == old and consumer.offset() == 2


def test_later_successor_is_supported_rollback_without_generation_rewind(tmp_path):
    store, old, route, consumer, lock = configured(tmp_path)
    transition(store, lock, old, [route])
    middle = store.read_current("session")
    middle_route = replace(route, session=middle.session)
    transition(store, lock, middle, [middle_route])
    current = store.read_current("session")
    assert current.session.ref.generation == 3
    assert TelegramInbox(store, [replace(route, session=current.session)]).offset() == 2
    with store._read() as c:
        assert historical_session_allows(store, c, current.session, old.session)
        assert historical_session_allows(store, c, current.session, middle.session)


def test_materialized_undispatched_schedule_blocks_transition(tmp_path):
    store, old, route, consumer, lock = configured(tmp_path)
    value = IntervalSchedule("s", old.session, old.goal.digest, 1000, 100, "input")
    ScheduleRepository(store).create(value)
    ScheduleOccurrences(store).enqueue(value, 1000, 1000)
    with pytest.raises(ContractError, match="undispatched"):
        transition(store, lock, old, [route])
    assert store.read_current("session") == old and consumer.offset() == 2
    assert ScheduleRepository(store).read(old.session, "s") == value


def test_ordinary_ancestry_rejects_a_forged_cross_domain_edge(tmp_path):
    from ahc.infrastructure.runtime_recovery_access import verified_continuation_edges
    from ahc.infrastructure.working_db import insert_snapshot
    from ahc.infrastructure.working_repository import WorkingSetCorrupt
    store, old, route, consumer, lock = configured(tmp_path)
    valid = old.continue_as(old.session.successor(), "backend-rollover")
    forged = replace(valid, session=replace(valid.session, route_id="foreign"), snapshot_digest=None)
    store._mutate(lambda c: insert_snapshot(c, forged))
    with store._read() as c:
        with pytest.raises(WorkingSetCorrupt, match="provenance"):
            verified_continuation_edges(c, "session")


def test_new_session_reads_projected_checkpoint_without_old_write_authority(tmp_path):
    from ahc.application.runtime_service import RuntimeService
    from test_runtime_workspace import Port
    store, old, route, consumer, lock = configured(tmp_path)
    with RuntimeService(store, None).open_workspace(old, "work", Port) as workspace:
        workspace.port.value = 73
        checkpoint = workspace.checkpoint(workspace.identity, ("x",))
    transition(store, lock, old, [route])
    current = store.read_current("session")
    projected = store.read_workspace_checkpoint(current.session, "work", checkpoint.digest)
    assert projected.origin.session == current.session
    assert projected.values_json == checkpoint.values_json
    with pytest.raises(WorkingSetConflict):
        store.read_workspace_checkpoint(old.session, "work", checkpoint.digest)


def test_durable_held_submission_stays_archived_after_continuation(tmp_path):
    store, old, route, consumer, lock = configured(tmp_path)
    RuntimeInbox(store).submit(old.session, "held", old.goal.digest, "do not replay")
    def hold(c):
        c.execute("CREATE TABLE channel_held (operation_id TEXT PRIMARY KEY)")
        c.execute("INSERT INTO channel_held VALUES('held')")
    store._mutate(hold)
    transition(store, lock, old, [route])
    current = store.read_current("session")
    assert RuntimeInbox(store).next_pending(current.session) is None
    with store._read() as c:
        assert c.execute("SELECT count(*) FROM channel_held").fetchone()[0] == 1
        assert c.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1


def held_fixture(tmp_path):
    from ahc.infrastructure.telegram_hold import hold_updates

    store, old, route, consumer, lock = configured(tmp_path)
    hold_updates(consumer, [{"update_id": 2, "message": {"unsupported": True}}], 2)
    with store._read() as connection:
        rows = [tuple(row) for row in connection.execute("SELECT * FROM runtime_telegram_holds")]
    return store, old, route, consumer, lock, rows


def test_retained_raw_hold_survives_forward_and_rollback_without_replay(tmp_path):
    store, old, route, consumer, lock, rows = held_fixture(tmp_path)
    first = transition(store, lock, old, [route], expected_offset=3)
    current = store.read_current("session")
    successor_route = replace(route, session=current.session)
    assert first["retained_telegram_holds"]["count"] == 1
    assert TelegramInbox(store, [successor_route]).offset() == 3
    assert RuntimeInbox(store).next_pending(current.session) is None
    # A later forward generation can retain the same original quarantine bytes.
    second = transition(store, lock, current, [successor_route], expected_offset=3)
    assert second["retained_telegram_holds"] == first["retained_telegram_holds"]
    latest = store.read_current("session")
    assert latest.session.ref.generation == 3
    assert RuntimeInbox(store).next_pending(latest.session) is None
    with store._read() as connection:
        assert [
            tuple(row) for row in connection.execute("SELECT * FROM runtime_telegram_holds")
        ] == rows
        exists = connection.execute(
            "SELECT name FROM sqlite_master WHERE name='runtime_submissions'"
        ).fetchone()
        assert (
            not exists
            or connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 0
        )


@pytest.mark.parametrize(
    "variant", ["account", "route", "session", "future", "payload_id", "payload", "cursor"]
)
def test_retained_raw_hold_rejects_unproven_custody_before_mutation(tmp_path, variant):
    from ahc.domain.skill_values import canonical_json

    store, old, route, consumer, lock, _ = held_fixture(tmp_path)

    def corrupt(connection):
        if variant == "account":
            connection.execute("UPDATE runtime_telegram_holds SET account_id='999'")
        elif variant in {"route", "session"}:
            foreign = (
                replace(route, user_id="999")
                if variant == "route"
                else replace(route, session=replace(old.session, route_id="foreign"))
            )
            connection.execute(
                "UPDATE runtime_telegram_holds SET routes_json=?",
                (canonical_json([foreign.to_dict()]).decode(),),
            )
        elif variant == "future":
            connection.execute("UPDATE runtime_telegram_holds SET update_id=3")
        elif variant == "payload_id":
            connection.execute(
                "UPDATE runtime_telegram_holds SET update_json=?",
                (canonical_json({"update_id": 1}),),
            )
        elif variant == "payload":
            connection.execute("UPDATE runtime_telegram_holds SET update_json=?", (b"{",))
        else:
            connection.execute("DELETE FROM runtime_telegram_cursors")

    store._mutate(corrupt)
    with pytest.raises(ContractError, match="runtime_telegram_holds"):
        transition(store, lock, old, [route], expected_offset=3)
    assert store.read_current("session") == old


def test_continuation_copies_exact_preferences_and_rolls_back_atomically(tmp_path):
    from ahc.domain.skill_values import canonical_json

    store, old, route, consumer, lock = configured(tmp_path)
    payload = canonical_json({"model": "chosen-model", "effort": "max", "revision": 7}).decode()
    original_key = canonical_json(route.to_dict()).decode()

    def seed(connection):
        connection.execute(
            "CREATE TABLE channel_preferences (route_json TEXT PRIMARY KEY,payload_json TEXT NOT NULL)"
        )
        connection.execute("INSERT INTO channel_preferences VALUES(?,?)", (original_key, payload))

    store._mutate(seed)
    with pytest.raises(RuntimeError, match="precommit"):
        transition(store, lock, old, [route], fail_before_commit=True)
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM channel_preferences").fetchone()[0] == 1
    result = transition(store, lock, old, [route])
    new_key = canonical_json(replace(route, session=old.session.successor()).to_dict()).decode()
    with store._read() as connection:
        assert dict(connection.execute("SELECT * FROM channel_preferences")) == {
            original_key: payload,
            new_key: payload,
        }
    assert result["copied_preferences"] == 1


def test_conflicting_successor_preferences_block_the_entire_continuation(tmp_path):
    from ahc.domain.skill_values import canonical_json

    store, old, route, consumer, lock = configured(tmp_path)

    def seed(connection):
        connection.execute(
            "CREATE TABLE channel_preferences (route_json TEXT PRIMARY KEY,payload_json TEXT NOT NULL)"
        )
        for owner, value in ((old.session, "original"), (old.session.successor(), "foreign")):
            connection.execute(
                "INSERT INTO channel_preferences VALUES(?,?)",
                (canonical_json(replace(route, session=owner).to_dict()).decode(), value),
            )

    store._mutate(seed)
    with pytest.raises(ContractError, match="preferences conflict"):
        transition(store, lock, old, [route])
    assert store.read_current("session") == old and consumer.offset() == 2
