"""Atomic occurrence-to-inbox admission; ordinary foreground owns execution."""

from ahc.domain.schedule_dispatch import SCHEDULE_TYPES, is_occurrence
from ahc.domain.schedules import MAX_SAFE_INTEGER
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json

from .channel_schedule_routes import ChannelScheduleRoutes
from .runtime_inbox import RuntimeInbox
from .schedule_repository import ScheduleRepository


class ScheduleOccurrences:
    def __init__(self, store):
        self.store = store

    def enqueue(self, expected, due_ms, now_ms, *, fail_before_commit=False, _transaction=None):
        if (type(expected) not in SCHEDULE_TYPES or expected.state != "active"
                or type(due_ms) is not int or type(now_ms) is not int
                or not 0 <= due_ms <= now_ms <= MAX_SAFE_INTEGER
                or not is_occurrence(expected, due_ms)):
            raise ContractError("schedule occurrence is not due")
        def write(connection):
            schedules = ScheduleRepository(self.store)
            schedules._owner(connection, expected, activate=True)
            row = connection.execute("SELECT * FROM runtime_schedules WHERE schedule_id=?",
                                     (expected.schedule_id,)).fetchone()
            if row is None or schedules._decode(row) != expected:
                raise ContractError("schedule occurrence revision stale")
            route = ChannelScheduleRoutes.route_for(connection, expected)
            event = None
            if route is None:
                operation_id = "schedule-" + digest_json({"schedule_id": expected.schedule_id,
                    "session": expected.session.to_dict(), "goal_digest": expected.goal_digest,
                    "due_ms": due_ms})
            else:
                from ahc.domain.channel_ingress import ChannelInboundEvent

                event = ChannelInboundEvent(
                    route, "schedule-" + digest_json({"schedule_id": expected.schedule_id,
                    "due_ms": due_ms}), expected.input_text)
                operation_id = event.operation_id
            prior = connection.execute(
                "SELECT * FROM runtime_schedule_occurrences WHERE schedule_id=? AND due_ms=?",
                (expected.schedule_id, due_ms),
            ).fetchone()
            if prior is not None and prior["operation_id"] != operation_id:
                raise ContractError("schedule occurrence binding corrupted")
            if prior is not None and connection.execute(
                "SELECT 1 FROM runtime_submissions WHERE operation_id=?", (operation_id,),
            ).fetchone() is None:
                raise ContractError("schedule occurrence submission is missing")
            receipt = RuntimeInbox(self.store).submit(expected.session, operation_id,
                expected.goal_digest, expected.input_text, channel_event=event,
                _transaction=connection, _skip_channel_controls=True)
            if prior is None:
                connection.execute("INSERT INTO runtime_schedule_occurrences VALUES(?,?,?,?)",
                    (expected.schedule_id, due_ms, operation_id, expected.revision))
            return {"schedule_id": expected.schedule_id, "due_ms": due_ms,
                    **receipt, "already_submitted": prior is not None}
        if _transaction is not None:
            return write(_transaction)
        return self.store._mutate(write, fail_before_commit=fail_before_commit)
