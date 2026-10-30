"""Explicit reversible local effects with the existing durable batch receipts."""

import json

from ahc.application.tool_batch import BatchRejected
from ahc.domain.capability_values import EffectClass
from ahc.domain.sessions import ContractError, _identifier

from .runtime_query_port import RuntimeQueryPort
from .runtime_read_batch import RuntimeReadBatch
from .schedule_repository import ScheduleRepository

PAUSE = "runtime.schedule.pause"


class RuntimeLocalPort(RuntimeQueryPort):
    def invoke(self, session, call, deadline, cancelled):
        if call.capability_id != PAUSE:
            return super().invoke(session, call, deadline, cancelled)
        try:
            value = json.loads(call.arguments_json)
            if (session != self.session or call.resource_id != "runtime"
                    or set(value) != {"schedule_id", "expected_revision"}
                    or type(value["expected_revision"]) is not int
                    or value["expected_revision"] < 1):
                raise ValueError()
            _identifier(value["schedule_id"], "schedule")
            schedules = ScheduleRepository(self.store)
            expected = schedules.read(session, value["schedule_id"])
            if (expected is None or expected.revision != value["expected_revision"]
                    or cancelled() or self.clock() >= deadline):
                raise ValueError()
        except (ContractError, ValueError, TypeError):
            raise BatchRejected("schedule pause request invalid") from None
        successor = schedules.set_state(expected, "paused")
        return {"schedule_id": successor.schedule_id, "revision": successor.revision,
                "state": successor.state}


class RuntimeLocalBatch(RuntimeReadBatch):
    def __init__(self, store, session, workspace_id, view):
        super().__init__(store, session, workspace_id, view)
        self.port = RuntimeLocalPort(store, session)
        self.port.clock = lambda: self.clock()

    def _effect(self, call):
        return (EffectClass.REVERSIBLE_LOCAL if call.capability_id == PAUSE
                else EffectClass.READ_ONLY)

    def run(self, expected, calls, *, deadline, cancelled=lambda: False, concurrency=1):
        if concurrency != 1 and any(
                getattr(call, "capability_id", None) == PAUSE for call in calls):
            raise ContractError("effectful local batches require sequential execution")
        return super().run(expected, calls, deadline=deadline, cancelled=cancelled,
                           concurrency=concurrency)
