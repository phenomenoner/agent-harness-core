import contextlib
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_controls import make

from ahc.adapters.codex_steering import turn_steer_message
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt, WorkingWriterBusy


def steering_fixture(tmp_path):
    store, current, request, route, controls, _, submit = make(tmp_path)
    active = submit("active work")
    request = replace(request, operation_id=active.operation_id, input_text=active.input_text)
    store.begin(current, request)
    submit("/messages steer")
    directed = submit("guide this turn")
    sent = []
    client = SimpleNamespace(thread="thread-1", turn="turn-1", _send=sent.append)
    return store, request, directed, controls.steering_port(request), client, sent


def steering_state(store, operation_id):
    with store._read() as connection:
        return connection.execute(
            "SELECT state,thread_id,expected_turn_id,wire_id FROM channel_steering "
            "WHERE operation_id=?",
            (operation_id,),
        ).fetchone()


def test_sqlite_reader_busy_rolls_back_steer_claim_then_sends_once_after_release(tmp_path):
    store, request, directed, port, client, sent = steering_fixture(tmp_path)
    reader = sqlite3.connect(store.db_path, isolation_level=None, timeout=0.0)
    reader.execute("BEGIN")
    reader.execute("SELECT * FROM channel_steering").fetchall()
    try:
        port.poll(client)
        assert sent == []
        assert tuple(steering_state(store, directed.operation_id)) == (
            "pending", None, None, None
        )
    finally:
        reader.rollback()
        reader.close()

    port.poll(client)
    assert sent == [
        turn_steer_message(
            "steer-" + digest_json(directed.operation_id),
            "thread-1",
            "turn-1",
            "guide this turn",
        )
    ]


def test_writer_lock_busy_defers_preclaim_without_sending(tmp_path):
    store, _, directed, port, client, sent = steering_fixture(tmp_path)
    holder = RuntimeRepository(store.root)
    with holder.writer_lock():
        port.poll(client)
    assert sent == []
    assert steering_state(store, directed.operation_id)["state"] == "pending"

    port.poll(client)
    assert len(sent) == 1


def test_idle_steer_probe_skips_mutation(tmp_path, monkeypatch):
    _, _, request, _, controls, _, _ = make(tmp_path)
    port = controls.steering_port(request)
    client = SimpleNamespace(thread="thread-1", turn="turn-1", _send=lambda _: None)
    monkeypatch.setattr(
        controls.store,
        "_mutate",
        lambda operation: pytest.fail("idle steer probe entered mutation"),
    )
    port.poll(client)


def test_idle_restart_probe_skips_mutation(tmp_path, monkeypatch):
    _, _, _, _, controls, _, _ = make(tmp_path)
    monkeypatch.setattr(
        controls.store,
        "_mutate",
        lambda operation: pytest.fail("idle restart probe entered mutation"),
    )
    assert controls.restart_ready() is False


def test_steer_claim_revalidates_after_probe_race(tmp_path, monkeypatch):
    store, _, directed, port, client, sent = steering_fixture(tmp_path)
    original_read = store._read
    raced = False

    @contextlib.contextmanager
    def read_then_other_claim():
        nonlocal raced
        with original_read() as connection:
            yield connection
        if not raced:
            raced = True
            store._mutate(
                lambda connection: connection.execute(
                    "UPDATE channel_steering SET state='sending',thread_id=?,"
                    "expected_turn_id=?,wire_id=? WHERE operation_id=? AND state='pending'",
                    ("other-thread", "other-turn", "other-wire", directed.operation_id),
                )
            )

    monkeypatch.setattr(store, "_read", read_then_other_claim)
    port.poll(client)

    assert sent == []
    assert tuple(steering_state(store, directed.operation_id)) == (
        "sending", "other-thread", "other-turn", "other-wire"
    )


@pytest.mark.parametrize("error", [WorkingSetCorrupt("corrupt"), RuntimeError("other")])
def test_nonbusy_steer_probe_errors_are_not_swallowed(tmp_path, monkeypatch, error):
    store, _, _, port, client, _ = steering_fixture(tmp_path)

    @contextlib.contextmanager
    def broken_read():
        raise error
        yield

    monkeypatch.setattr(store, "_read", broken_read)
    with pytest.raises(type(error), match=str(error)):
        port.poll(client)


def test_nonbusy_steer_claim_errors_are_not_swallowed(tmp_path, monkeypatch):
    store, _, _, port, client, _ = steering_fixture(tmp_path)
    monkeypatch.setattr(
        store, "_mutate", lambda operation: (_ for _ in ()).throw(RuntimeError("other"))
    )
    with pytest.raises(RuntimeError, match="other"):
        port.poll(client)


def test_restart_transaction_busy_propagates_after_read_probe(tmp_path):
    import sqlite3 as sqlite

    store, _, _, route, controls, _, submit = make(tmp_path)
    event = submit("/restart")
    controls.deliver_one({route: SimpleNamespace(send=lambda *args: "receipt")})
    reader = sqlite.connect(store.db_path, isolation_level=None, timeout=0.0)
    reader.execute("BEGIN")
    reader.execute("SELECT * FROM channel_controls").fetchall()
    try:
        with pytest.raises(WorkingWriterBusy):
            controls.restart_ready()
    finally:
        reader.rollback()
        reader.close()
    assert controls.restart_ready() is True
    assert controls.restart_ready() is False
    with store._read() as connection:
        assert tuple(
            connection.execute(
                "SELECT operation_id,restart,state FROM channel_controls WHERE operation_id=?",
                (event.operation_id,),
            ).fetchone()
        ) == (event.operation_id, 2, "sent")
