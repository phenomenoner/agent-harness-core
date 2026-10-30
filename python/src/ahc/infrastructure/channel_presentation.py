"""Presentation follows durable task provenance, never task text or elapsed time."""

from .runtime_channel_store import _exists


def operation_kind(connection, operation_id):
    if (_exists(connection, "runtime_background_dispatch") and connection.execute(
            "SELECT 1 FROM runtime_background_dispatch WHERE worker_operation_id=?",
            (operation_id,)).fetchone() is not None):
        return "scheduled"
    if (_exists(connection, "runtime_schedule_occurrences")
            and connection.execute(
                "SELECT 1 FROM runtime_schedule_occurrences WHERE operation_id=?",
                (operation_id,),
            ).fetchone() is not None):
        return "scheduled"
    return "reply"


def background_operation(store, operation_id):
    with store._read() as connection:
        return operation_kind(connection, operation_id) == "scheduled"
