"""Bounded admission-unavailability notices retain channel submissions."""

from test_channel_controls import make

from ahc.application.channel_ingress import ChannelIngress
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.infrastructure.channel_availability import queue_unavailable_notices
from ahc.infrastructure.runtime_channel_store import bind_submission
from ahc.infrastructure.runtime_inbox import _TABLE, RuntimeInbox


def _control_rows(store):
    with store._read() as connection:
        return connection.execute(
            "SELECT * FROM channel_controls WHERE operation_id LIKE 'availability-%' "
            "ORDER BY rowid"
        ).fetchall()


def _insert_pending(store, event, goal_digest):
    payload = RuntimeInbox._payload(event.route.session, event.operation_id,
                                    goal_digest, event.input_text)
    encoded = canonical_json(payload).decode()
    fingerprint = digest_json(payload)

    def write(connection):
        connection.execute(_TABLE)
        connection.execute(
            "INSERT INTO runtime_submissions VALUES(?,?,?,?,?)",
            (event.operation_id, canonical_json(event.route.session.to_dict()).decode(),
             goal_digest, encoded, fingerprint),
        )
        bind_submission(connection, event, existing=False)

    store._mutate(write)


def test_unavailable_notices_cover_normal_and_quoted_pending_messages(tmp_path):
    store, current, _, route, controls, _, submit = make(tmp_path)
    normal = submit("normal work", "normal")
    quoted = ChannelInboundEvent(
        route, "quoted", "quoted work", context={"quote": "quoted context"}
    )
    ChannelIngress(RuntimeInbox(store), route).submit(quoted, current.goal.digest)

    assert queue_unavailable_notices(
        controls,
        {"ready": False, "reason": "workspace-unavailable", "trace": "PRIVATE"},
    ) == 2
    rows = _control_rows(store)
    assert {row["route_json"] for row in rows} == {
        canonical_json(route.to_dict()).decode()
    }
    assert {row["operation_id"] for row in rows}.isdisjoint(
        {normal.operation_id, quoted.operation_id}
    )
    assert all(row["state"] == "pending" for row in rows)
    assert all("PRIVATE" not in row["reply"] for row in rows)
    assert all("訊息已保留" in row["reply"] for row in rows)
    assert all("尚未執行" in row["reply"] and "/status" in row["reply"] for row in rows)
    assert queue_unavailable_notices(
        controls, {"ready": False, "reason": "workspace-unavailable"}
    ) == 0
    assert len(_control_rows(store)) == 2


def test_delivery_of_an_unknown_notice_is_not_replayed(tmp_path):
    store, current, _, route, controls, _, _ = make(tmp_path)
    event = ChannelInboundEvent(
        route, "quoted-only", "quoted work", context={"quote": "quoted context"}
    )
    ChannelIngress(RuntimeInbox(store), route).submit(event, current.goal.digest)
    controls.bind_admission_status(
        lambda: {"ready": False, "reason": "backend-cleanup-pending"}
    )
    calls = []

    class Transport:
        def send(self, *args):
            calls.append(args)
            raise RuntimeError("ambiguous provider outcome")

    controls.deliver_one({route: Transport()})
    controls.deliver_one({route: Transport()})

    assert len(calls) == 1
    rows = _control_rows(store)
    assert len(rows) == 1 and rows[0]["state"] == "unknown"
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_turns").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1


def test_bounded_scan_advances_past_existing_notice_rows(tmp_path):
    store, current, _, route, controls, _, _ = make(tmp_path)
    for index in range(65):
        _insert_pending(
            store,
            ChannelInboundEvent(route, f"bounded-{index}", f"work {index}"),
            current.goal.digest,
        )

    observation = {"ready": False, "reason": "workspace-unavailable"}
    assert queue_unavailable_notices(controls, observation) == 64
    assert len(_control_rows(store)) == 64
    assert queue_unavailable_notices(controls, observation) == 1
    assert len(_control_rows(store)) == 65


def test_held_admitted_foreign_and_wrong_goal_submissions_are_not_warned(tmp_path):
    store, current, _, route, controls, _, submit = make(tmp_path)
    held = submit("held", "held")
    admitted = submit("admitted", "admitted")
    wrong_goal = ChannelInboundEvent(route, "wrong-goal", "wrong goal")
    _insert_pending(store, wrong_goal, "0" * 64)
    foreign_route = ChannelRoute("telegram", "1", "2", "foreign", current.session)
    foreign = ChannelInboundEvent(foreign_route, "foreign", "foreign")
    _insert_pending(store, foreign, current.goal.digest)

    def mark_excluded(connection):
        connection.execute("INSERT INTO channel_held VALUES(?)", (held.operation_id,))
        connection.execute(
            "INSERT INTO runtime_turns VALUES(?,?,?,?,?,?)",
            (admitted.operation_id, "d" * 64,
             canonical_json(current.session.to_dict()).decode(), "{}", "submitted", None),
        )

    store._mutate(mark_excluded)
    assert queue_unavailable_notices(
        controls, {"ready": False, "reason": "runtime-not-running"}
    ) == 0
    assert _control_rows(store) == []


def test_ready_observation_does_not_mutate_controls(tmp_path, monkeypatch):
    store, _, _, _, controls, _, _ = make(tmp_path)
    calls = []

    def forbidden(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("ready observation must not open a mutation")

    monkeypatch.setattr(store, "_mutate", forbidden)
    assert queue_unavailable_notices(
        controls, {"ready": True, "reason": "ready", "path": "PRIVATE"}
    ) == 0
    assert calls == []
