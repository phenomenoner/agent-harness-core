"""Read-only interval progress; persisted admissions are not execution success."""

from ahc.domain.schedule_dispatch import backlog, following, is_occurrence
from ahc.domain.schedules import MAX_SAFE_INTEGER
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier

from .schedule_repository import ScheduleRepository


def inspect_schedule(store, session, schedule_id, now_ms):
    if (type(session) is not VirtualSessionIdentity or type(now_ms) is not int
            or not 0 <= now_ms <= MAX_SAFE_INTEGER):
        raise ContractError("schedule status input invalid")
    _identifier(schedule_id, "schedule")
    with store._read() as connection:
        connection.execute("BEGIN")
        row = connection.execute("SELECT * FROM runtime_schedules WHERE schedule_id=?",
                                 (schedule_id,)).fetchone()
        if row is None:
            return {"status": "absent"}
        value = ScheduleRepository._decode(row)
        if value.session != session:
            raise ContractError("schedule status owner mismatch")
        cursor = connection.execute(
            "SELECT through_ms FROM runtime_schedule_cursors WHERE schedule_id=?",
            (schedule_id,),
        ).fetchone()
        through = None if cursor is None else cursor[0]
        if through is not None:
            if (not is_occurrence(value, through)):
                raise ContractError("schedule progress corrupted")
            backlink = connection.execute(
                "SELECT 1 FROM runtime_schedule_occurrences o JOIN runtime_submissions s "
                "ON s.operation_id=o.operation_id WHERE o.schedule_id=? AND o.due_ms=?",
                (schedule_id, through),
            ).fetchone()
            if backlink is None:
                raise ContractError("schedule progress backlink missing")
        current = store._load_current(connection, session.ref.virtual_session_id)
        eligible = (current.session == session and current.status == "active"
                    and current.goal.digest == value.goal_digest and value.state == "active")
        next_due = following(value, through)
        overdue = backlog(value, next_due, now_ms)
        count = connection.execute(
            "SELECT count(*) FROM runtime_schedule_occurrences WHERE schedule_id=?",
            (schedule_id,),
        ).fetchone()[0]
        return {"status": "observed", "schedule_id": schedule_id, "state": value.state,
                "revision": value.revision, "eligible": eligible, "through_ms": through,
                "next_cursor_due_ms": next_due,
                "overdue_cursor_slots": overdue, "recorded_occurrences": count,
                "observation_scope": "persisted-admission-progress"}
