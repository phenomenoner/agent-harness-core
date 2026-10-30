"""Persisted daily schedules carry the exact timezone bytes used for recurrence."""

import base64
import binascii
from dataclasses import dataclass, replace
from typing import ClassVar

from .calendar_rules import MAX_CALENDAR_MS, DailyCalendar
from .schedules import MAX_SAFE_INTEGER, IntervalSchedule, _state
from .sessions import ContractError, VirtualSessionIdentity, _exact_mapping


@dataclass(frozen=True, slots=True)
class DailySchedule:
    schedule_id: str
    session: VirtualSessionIdentity
    goal_digest: str
    start_ms: int
    input_text: str
    timezone: str
    hour: int
    minute: int
    tzif_base64: str
    overlap: str = "first"
    gap: str = "skip"
    revision: int = 1
    state: str = "active"
    schema_version: ClassVar[str] = "ahc.daily-schedule.v1"

    def __post_init__(self):
        IntervalSchedule(self.schedule_id, self.session, self.goal_digest, self.start_ms,
                         1, self.input_text, self.revision, self.state)
        if not 1 <= self.start_ms <= MAX_CALENDAR_MS:
            raise ContractError("daily start outside supported range")
        self.rule()

    def rule(self):
        try:
            if type(self.tzif_base64) is not str or len(self.tzif_base64) > 21848:
                raise ValueError()
            raw = base64.b64decode(self.tzif_base64, validate=True)
            if base64.b64encode(raw).decode() != self.tzif_base64:
                raise ValueError()
        except (ValueError, binascii.Error):
            raise ContractError("calendar timezone encoding invalid") from None
        return DailyCalendar(self.timezone, self.hour, self.minute, self.overlap, self.gap, raw)

    def next_after(self, after_ms):
        if self.state != "active":
            return None
        if type(after_ms) is not int or not 0 <= after_ms <= MAX_SAFE_INTEGER:
            raise ContractError("daily observation invalid")
        if after_ms > MAX_CALENDAR_MS:
            return None
        return self.rule().next_after(max(after_ms, self.start_ms - 1))

    def with_state(self, state):
        _state(state)
        if state == self.state:
            return self
        if self.state == "removed" or self.revision == MAX_SAFE_INTEGER:
            raise ContractError("daily state transition invalid")
        return replace(self, state=state, revision=self.revision + 1)

    def to_dict(self):
        result = {name: getattr(self, name) for name in self.__dataclass_fields__
                  if name != "schema_version"}
        result.update(schema_version=self.schema_version, session=self.session.to_dict())
        return result

    @classmethod
    def from_dict(cls, value):
        raw = _exact_mapping(value, set(cls.__dataclass_fields__), "daily schedule")
        if raw["schema_version"] != cls.schema_version:
            raise ContractError("daily schema invalid")
        fields = dict(raw)
        fields.pop("schema_version")
        fields["session"] = VirtualSessionIdentity.from_dict(fields["session"])
        return cls(**fields)
