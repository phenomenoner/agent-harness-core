"""Pure interval schedule intent values; execution is owned by a higher layer."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar, cast

from .sessions import (
    ContractError,
    VirtualSessionIdentity,
    _exact_mapping,
    _identifier,
    _schema_version,
)
from .turns import _sha256_hex

INTERVAL_SCHEDULE_SCHEMA_VERSION = "ahc.interval-schedule.v1"
INTERVAL_SCHEDULE_STATES = frozenset({"active", "paused", "removed"})
MAX_SAFE_INTEGER = 2**53 - 1
MAX_INTERVAL_MS = 86_400_000
_INPUT_TEXT_MAX_UTF8_BYTES = 4_096
_FIELDS = frozenset(
    {
        "schema_version",
        "schedule_id",
        "session",
        "goal_digest",
        "start_ms",
        "interval_ms",
        "input_text",
        "revision",
        "state",
    }
)


def _bounded_integer(value: object, label: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ContractError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return cast(int, value)


def _input_text(value: object) -> str:
    if type(value) is not str or not value:
        raise ContractError("input_text must be a non-empty string")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ContractError("input_text must contain no surrogate code points")
    if len(value.encode("utf-8")) > _INPUT_TEXT_MAX_UTF8_BYTES:
        raise ContractError("input_text must contain at most 4096 UTF-8 bytes")
    return value


def _state(value: object) -> str:
    if type(value) is not str or value not in INTERVAL_SCHEDULE_STATES:
        raise ContractError("state must be one of 'active', 'paused', or 'removed'")
    return value


@dataclass(frozen=True, slots=True)
class IntervalSchedule:
    """An immutable, deterministic recurring schedule intent."""

    schedule_id: str
    session: VirtualSessionIdentity
    goal_digest: str
    start_ms: int
    interval_ms: int
    input_text: str
    revision: int = 1
    state: str = "active"

    schema_version: ClassVar[str] = INTERVAL_SCHEDULE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _identifier(self.schedule_id, "schedule_id")
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("session must be a VirtualSessionIdentity")
        try:
            _sha256_hex(self.goal_digest)
        except ContractError:
            raise ContractError("goal_digest must be a lowercase SHA-256 hex string") from None
        _bounded_integer(self.start_ms, "start_ms", 0, MAX_SAFE_INTEGER)
        _bounded_integer(self.interval_ms, "interval_ms", 1, MAX_INTERVAL_MS)
        _input_text(self.input_text)
        _bounded_integer(self.revision, "revision", 1, MAX_SAFE_INTEGER)
        _state(self.state)

    def to_dict(self) -> dict[str, object]:
        """Emit a fresh exact-key JSON-shaped mapping."""

        return {
            "schema_version": INTERVAL_SCHEDULE_SCHEMA_VERSION,
            "schedule_id": self.schedule_id,
            "session": self.session.to_dict(),
            "goal_digest": self.goal_digest,
            "start_ms": self.start_ms,
            "interval_ms": self.interval_ms,
            "input_text": self.input_text,
            "revision": self.revision,
            "state": self.state,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> IntervalSchedule:
        """Parse and validate one exact-key JSON-shaped mapping."""

        raw = _exact_mapping(value, _FIELDS, "interval schedule")
        _schema_version(
            raw["schema_version"], INTERVAL_SCHEDULE_SCHEMA_VERSION, "interval schedule"
        )
        try:
            session = VirtualSessionIdentity.from_dict(
                cast(Mapping[str, object], raw["session"])
            )
        except ContractError as exc:
            raise ContractError(f"session: {exc}") from exc
        return cls(
            cast(str, raw["schedule_id"]),
            session,
            cast(str, raw["goal_digest"]),
            cast(int, raw["start_ms"]),
            cast(int, raw["interval_ms"]),
            cast(str, raw["input_text"]),
            cast(int, raw["revision"]),
            cast(str, raw["state"]),
        )

    def next_after(self, after_ms: int) -> int | None:
        """Return the first occurrence strictly after ``after_ms``, if representable."""

        after_ms = _bounded_integer(after_ms, "after_ms", 0, MAX_SAFE_INTEGER)
        if self.state != "active":
            return None
        if after_ms < self.start_ms:
            return self.start_ms
        occurrence_number = (after_ms - self.start_ms) // self.interval_ms + 1
        next_ms = self.start_ms + occurrence_number * self.interval_ms
        return next_ms if next_ms <= MAX_SAFE_INTEGER else None

    def with_state(self, state: str) -> IntervalSchedule:
        """Return a revisioned state transition while preserving schedule identity."""

        state = _state(state)
        if state == self.state:
            return self
        if self.state == "removed":
            raise ContractError("removed schedules cannot return to active or paused")
        if self.revision == MAX_SAFE_INTEGER:
            raise ContractError("schedule revision overflow")
        return type(self)(
            self.schedule_id,
            self.session,
            self.goal_digest,
            self.start_ms,
            self.interval_ms,
            self.input_text,
            self.revision + 1,
            state,
        )


__all__ = [
    "INTERVAL_SCHEDULE_SCHEMA_VERSION",
    "INTERVAL_SCHEDULE_STATES",
    "IntervalSchedule",
    "MAX_INTERVAL_MS",
    "MAX_SAFE_INTEGER",
]
