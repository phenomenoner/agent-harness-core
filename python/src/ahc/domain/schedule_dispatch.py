"""Closed schedule union and bounded recurrence traversal helpers."""

from dataclasses import replace

from .daily_schedules import DailySchedule
from .schedules import MAX_SAFE_INTEGER, IntervalSchedule

SCHEDULE_TYPES = (IntervalSchedule, DailySchedule)


def parse_schedule(value):
    if type(value) is dict and value.get("schema_version") == DailySchedule.schema_version:
        return DailySchedule.from_dict(value)
    return IntervalSchedule.from_dict(value)


def following(value, through):
    active = replace(value, state="active")
    if through is None:
        return value.start_ms if type(value) is IntervalSchedule else active.next_after(
            value.start_ms - 1)
    return active.next_after(through)


def is_occurrence(value, due):
    if type(due) is not int or not value.start_ms <= due <= MAX_SAFE_INTEGER:
        return False
    if type(value) is IntervalSchedule:
        return (due - value.start_ms) % value.interval_ms == 0
    return following(value, due - 1) == due


def backlog(value, next_due, now):
    if next_due is None or next_due > now:
        return 0
    if type(value) is IntervalSchedule:
        return (now - next_due) // value.interval_ms + 1
    # Calendar counting is not constant-time. Null means an uncounted backlog,
    # never zero; callers still receive the next due instant.
    return None
