"""Idle-step interval admission using the foreground owner's existing clock loop."""

import time

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .schedule_repository import ScheduleRepository
from .schedule_tick import ScheduleTick
from .working_repository import WorkingRepositoryError


class ForegroundSchedule:
    def __init__(self, store, *, clock_ms=None):
        self.store = store
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.after = ""
        self.status = "idle"

    def __call__(self, current):
        # One candidate per idle step; rotate so a far-future or invalid schedule
        # cannot permanently starve another schedule. No raw errors enter status.
        try:
            with self.store._read() as connection:
                query = ("SELECT * FROM runtime_schedules WHERE session_json=? "
                         "AND state='active' AND schedule_id>? ORDER BY schedule_id LIMIT 1")
                session = canonical_json(current.session.to_dict()).decode()
                row = connection.execute(query, (session, self.after)).fetchone()
                if row is None:
                    self.after = ""
                    row = connection.execute(query, (session, "")).fetchone()
                if row is None:
                    self.status = "idle"
                    return
                self.after = row["schedule_id"]
                value = ScheduleRepository._decode(row)
            if value.goal_digest != current.goal.digest:
                self.status = "stale-goal"
                return
            result = ScheduleTick(self.store).tick(value, self.clock_ms(), limit=1)
            self.status = "submitted" if result["occurrences"] else "not-due"
        except (ContractError, WorkingRepositoryError):
            self.status = "admission-unavailable"
