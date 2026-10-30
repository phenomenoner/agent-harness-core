"""Bounded chronological catch-up; cursor and inbox admissions commit together."""

from ahc.domain.schedule_dispatch import SCHEDULE_TYPES, backlog, following, is_occurrence
from ahc.domain.schedules import MAX_SAFE_INTEGER
from ahc.domain.sessions import ContractError

from .schedule_occurrences import ScheduleOccurrences
from .schedule_repository import ScheduleRepository


class ScheduleTick:
    def __init__(self, store):
        self.store = store

    def tick(self, expected, now_ms, *, limit=16, fail_before_commit=False):
        if (type(expected) not in SCHEDULE_TYPES or expected.state != "active"
                or type(now_ms) is not int or not 0 <= now_ms <= MAX_SAFE_INTEGER
                or type(limit) is not int or not 1 <= limit <= 16):
            raise ContractError("schedule tick input invalid")

        def write(connection):
            schedules = ScheduleRepository(self.store)
            schedules._owner(connection, expected, activate=True)
            row = connection.execute("SELECT * FROM runtime_schedules WHERE schedule_id=?",
                                     (expected.schedule_id,)).fetchone()
            if row is None or schedules._decode(row) != expected:
                raise ContractError("schedule tick revision stale")
            cursor = connection.execute(
                "SELECT through_ms FROM runtime_schedule_cursors WHERE schedule_id=?",
                (expected.schedule_id,),
            ).fetchone()
            through = None if cursor is None else cursor[0]
            if through is not None:
                if (not is_occurrence(expected, through)
                        or connection.execute(
                            "SELECT 1 FROM runtime_schedule_occurrences "
                            "WHERE schedule_id=? AND due_ms=?",
                            (expected.schedule_id, through),
                        ).fetchone() is None):
                    raise ContractError("schedule cursor binding corrupted")
            next_due = following(expected, through)
            receipts = []
            occurrences = ScheduleOccurrences(self.store)
            while next_due is not None and next_due <= now_ms and len(receipts) < limit:
                receipts.append(occurrences.enqueue(expected, next_due, now_ms,
                                                     _transaction=connection))
                through, next_due = next_due, following(expected, next_due)
            if receipts:
                connection.execute(
                    "INSERT INTO runtime_schedule_cursors VALUES(?,?) "
                    "ON CONFLICT(schedule_id) DO UPDATE SET through_ms=excluded.through_ms",
                    (expected.schedule_id, through),
                )
            remaining = backlog(expected, next_due, now_ms)
            return {"schedule_id": expected.schedule_id, "through_ms": through,
                    "occurrences": receipts, "remaining_due": remaining,
                    "next_due_ms": next_due}
        return self.store._mutate(write, fail_before_commit=fail_before_commit)
