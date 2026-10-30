"""Explicit interval intent custody; no clock loop or automatic task dispatch."""

import json

from ahc.domain.schedule_dispatch import SCHEDULE_TYPES, parse_schedule
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.skill_values import canonical_json


class ScheduleRepository:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def _decode(row):
        try:
            value = parse_schedule(json.loads(row["schedule_json"]))
            if (value.schedule_id != row["schedule_id"] or value.revision != row["revision"]
                    or value.state != row["state"]
                    or canonical_json(value.session.to_dict()).decode() != row["session_json"]
                    or canonical_json(value.to_dict()).decode() != row["schedule_json"]):
                raise ValueError()
            return value
        except (ValueError, TypeError, KeyError):
            raise ContractError("schedule record corrupted") from None

    def _owner(self, connection, value, *, activate):
        current = self.store._load_current(connection, value.session.ref.virtual_session_id)
        if current.session != value.session or (activate and (
                current.status != "active" or current.goal.digest != value.goal_digest)):
            raise ContractError("schedule owner or active goal mismatch")

    def create(self, value, *, _transaction=None):
        if type(value) not in SCHEDULE_TYPES or value.revision != 1 or value.state != "active":
            raise ContractError("new schedule must be initially active")

        def write(connection):
            self._owner(connection, value, activate=True)
            row = connection.execute("SELECT * FROM runtime_schedules WHERE schedule_id=?",
                                     (value.schedule_id,)).fetchone()
            if row is not None:
                existing = self._decode(row)
                if existing != value:
                    raise ContractError("schedule identifier already bound")
                return existing
            connection.execute("INSERT INTO runtime_schedules VALUES(?,?,?,?,?)", (
                value.schedule_id, canonical_json(value.session.to_dict()).decode(),
                value.revision, value.state, canonical_json(value.to_dict()).decode()))
            return value
        return write(_transaction) if _transaction is not None else self.store._mutate(write)

    def read(self, session, schedule_id, *, _transaction=None):
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("schedule session invalid")
        _identifier(schedule_id, "schedule")
        def lookup(connection):
            row = connection.execute("SELECT * FROM runtime_schedules WHERE schedule_id=?",
                                     (schedule_id,)).fetchone()
            if row is None:
                return None
            value = self._decode(row)
            if value.session != session:
                raise ContractError("schedule read owner mismatch")
            return value
        if _transaction is not None:
            return lookup(_transaction)
        with self.store._read() as connection:
            return lookup(connection)

    def set_state(self, expected, state, *, _transaction=None):
        if type(expected) not in SCHEDULE_TYPES:
            raise ContractError("schedule expected value invalid")
        successor = expected.with_state(state)

        def write(connection):
            self._owner(connection, expected, activate=state == "active")
            row = connection.execute("SELECT * FROM runtime_schedules WHERE schedule_id=?",
                                     (expected.schedule_id,)).fetchone()
            if row is None or self._decode(row) != expected:
                raise ContractError("schedule expected revision is stale")
            connection.execute("UPDATE runtime_schedules SET revision=?,state=?,schedule_json=? "
                               "WHERE schedule_id=?", (successor.revision, successor.state,
                               canonical_json(successor.to_dict()).decode(), successor.schedule_id))
            return successor
        return write(_transaction) if _transaction is not None else self.store._mutate(write)

    def list_page(self, session, *, after=None, limit=50):
        """Read one exact-owner page in schedule-ID order, including removed history."""
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("schedule session invalid")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ContractError("schedule page limit must be 1-100")
        if after is not None:
            _identifier(after, "schedule cursor")
        with self.store._read() as connection:
            rows = connection.execute(
                "SELECT * FROM runtime_schedules WHERE session_json=? AND schedule_id>? "
                "ORDER BY schedule_id LIMIT ?",
                (canonical_json(session.to_dict()).decode(), after or "", limit + 1),
            ).fetchall()
            values = tuple(self._decode(row) for row in rows[:limit])
            return values, values[-1].schedule_id if len(rows) > limit else None
