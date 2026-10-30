"""Daily wall-clock recurrence with explicit DST gap and overlap semantics."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import BytesIO
from struct import error as StructError
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .sessions import ContractError

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# Leave one full year below datetime's upper bound for bounded date traversal.
MAX_CALENDAR_MS = 253_370_764_799_999


@dataclass(frozen=True, slots=True)
class DailyCalendar:
    timezone: str
    hour: int
    minute: int
    overlap: str = "first"
    gap: str = "skip"
    tzif: bytes | None = None

    def __post_init__(self):
        if (type(self.timezone) is not str or not self.timezone
                or len(self.timezone) > 128
                or type(self.hour) is not int or not 0 <= self.hour <= 23
                or type(self.minute) is not int or not 0 <= self.minute <= 59
                or self.overlap not in ("first", "second") or self.gap != "skip"):
            raise ContractError("daily calendar rule invalid")
        self.zone()

    def zone(self):
        try:
            if self.tzif is not None:
                if type(self.tzif) is not bytes or not 44 <= len(self.tzif) <= 16384:
                    raise ValueError()
                return ZoneInfo.from_file(BytesIO(self.tzif), key=self.timezone)
            return ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError, StructError, EOFError, AssertionError):
            raise ContractError("calendar timezone unavailable") from None

    def next_after(self, after_ms):
        if type(after_ms) is not int or not 0 <= after_ms <= MAX_CALENDAR_MS:
            raise ContractError("calendar instant outside supported range")
        zone = self.zone()
        after = _EPOCH + timedelta(milliseconds=after_ms)
        day = after.astimezone(zone).date()
        # A finite traversal also covers zones that skipped a whole civil day.
        for offset in range(370):
            date = day + timedelta(days=offset)
            wall = datetime(date.year, date.month, date.day, self.hour, self.minute)
            candidates = set()
            for fold in (0, 1):
                instant = wall.replace(tzinfo=zone, fold=fold).astimezone(UTC)
                if instant.astimezone(zone).replace(tzinfo=None) == wall:
                    delta = instant - _EPOCH
                    candidates.add(delta.days * 86_400_000 + delta.seconds * 1000)
            if not candidates:
                continue
            due = min(candidates) if self.overlap == "first" else max(candidates)
            if due > after_ms:
                return due if due <= MAX_CALENDAR_MS else None
        raise ContractError("calendar search bound exceeded")
