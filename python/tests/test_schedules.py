from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest

from ahc.domain.schedules import (
    INTERVAL_SCHEDULE_SCHEMA_VERSION,
    MAX_INTERVAL_MS,
    MAX_SAFE_INTEGER,
    IntervalSchedule,
)
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def _session() -> VirtualSessionIdentity:
    return VirtualSessionIdentity.initial("session-main", "agent-main", "route-main")


def _schedule(**overrides: object) -> IntervalSchedule:
    values: dict[str, object] = {
        "schedule_id": "schedule-main",
        "session": _session(),
        "goal_digest": "a" * 64,
        "start_ms": 100,
        "interval_ms": 50,
        "input_text": "run once",
    }
    values.update(overrides)
    return IntervalSchedule(**values)  # type: ignore[arg-type]


def test_round_trip_has_exact_schema_and_nested_session() -> None:
    value = _schedule()

    expected = {
        "schema_version": INTERVAL_SCHEDULE_SCHEMA_VERSION,
        "schedule_id": "schedule-main",
        "session": _session().to_dict(),
        "goal_digest": "a" * 64,
        "start_ms": 100,
        "interval_ms": 50,
        "input_text": "run once",
        "revision": 1,
        "state": "active",
    }
    emitted = value.to_dict()

    assert emitted == expected
    assert IntervalSchedule.from_dict(json.loads(json.dumps(emitted))) == value


def test_value_is_frozen_and_schema_is_class_level() -> None:
    value = _schedule()

    with pytest.raises(FrozenInstanceError):
        value.state = "paused"  # type: ignore[misc]
    assert value.schema_version == INTERVAL_SCHEDULE_SCHEMA_VERSION


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("schedule_id", True),
        ("schedule_id", ""),
        ("goal_digest", "A" * 64),
        ("goal_digest", "a" * 63),
        ("start_ms", True),
        ("start_ms", -1),
        ("start_ms", MAX_SAFE_INTEGER + 1),
        ("interval_ms", True),
        ("interval_ms", 0),
        ("interval_ms", MAX_INTERVAL_MS + 1),
        ("revision", False),
        ("revision", 0),
        ("revision", MAX_SAFE_INTEGER + 1),
        ("state", True),
        ("state", "unknown"),
    ],
)
def test_constructor_rejects_wrong_types_and_bounds(field: str, invalid: object) -> None:
    with pytest.raises(ContractError, match=field if field != "state" else "state"):
        _schedule(**{field: invalid})


def test_numeric_and_text_boundaries_are_accepted() -> None:
    value = _schedule(
        start_ms=MAX_SAFE_INTEGER,
        interval_ms=MAX_INTERVAL_MS,
        revision=MAX_SAFE_INTEGER,
        input_text="é" * 2_048,
    )

    assert value.start_ms == MAX_SAFE_INTEGER
    assert value.interval_ms == MAX_INTERVAL_MS
    assert len(value.input_text.encode("utf-8")) == 4_096


@pytest.mark.parametrize("input_text", ["", "é" * 2_049, "\ud800"])
def test_input_text_requires_nonempty_utf8_and_scalar_values(input_text: str) -> None:
    with pytest.raises(ContractError, match="input_text"):
        _schedule(input_text=input_text)


def test_next_after_is_strict_and_uses_exact_intervals() -> None:
    value = _schedule(start_ms=100, interval_ms=50)

    assert value.next_after(0) == 100
    assert value.next_after(99) == 100
    assert value.next_after(100) == 150
    assert value.next_after(149) == 150
    assert value.next_after(150) == 200


def test_next_after_uses_constant_time_arithmetic_at_safe_integer_boundary() -> None:
    value = _schedule(start_ms=0, interval_ms=1)

    assert value.next_after(MAX_SAFE_INTEGER - 1) == MAX_SAFE_INTEGER
    assert value.next_after(MAX_SAFE_INTEGER) is None


@pytest.mark.parametrize("after_ms", [True, -1, MAX_SAFE_INTEGER + 1])
def test_next_after_rejects_bool_and_out_of_range_after_values(after_ms: object) -> None:
    with pytest.raises(ContractError, match="after_ms"):
        _schedule().next_after(after_ms)  # type: ignore[arg-type]


def test_pause_resume_and_remove_are_revisioned_and_execution_free() -> None:
    active = _schedule()
    paused = active.with_state("paused")
    resumed = paused.with_state("active")
    removed = resumed.with_state("removed")

    assert paused.revision == 2
    assert paused.next_after(0) is None
    assert resumed.revision == 3
    assert resumed.next_after(100) == 150
    assert removed.revision == 4
    assert removed.next_after(0) is None
    assert removed.with_state("removed") is removed


def test_identical_state_returns_self_and_removed_cannot_reactivate() -> None:
    active = _schedule()
    paused = active.with_state("paused")

    assert active.with_state("active") is active
    assert paused.with_state("paused") is paused
    with pytest.raises(ContractError, match="removed"):
        paused.with_state("removed").with_state("active")
    with pytest.raises(ContractError, match="removed"):
        paused.with_state("removed").with_state("paused")


def test_state_transition_rejects_revision_overflow() -> None:
    value = _schedule(revision=MAX_SAFE_INTEGER)

    with pytest.raises(ContractError, match="overflow"):
        value.with_state("paused")


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload.pop("state"),
        lambda payload: payload.__setitem__("unexpected", None),
        lambda payload: payload.__setitem__("schema_version", "ahc.other.v1"),
        lambda payload: payload.__setitem__("schedule_id", 7),
        lambda payload: payload.__setitem__("session", None),
        lambda payload: payload.__setitem__("goal_digest", "A" * 64),
        lambda payload: payload.__setitem__("start_ms", True),
        lambda payload: payload.__setitem__("interval_ms", "50"),
        lambda payload: payload.__setitem__("input_text", None),
        lambda payload: payload.__setitem__("revision", 0),
        lambda payload: payload.__setitem__("state", "invalid"),
    ],
)
def test_from_dict_rejects_malformed_json_shaped_values(mutator) -> None:
    payload = _schedule().to_dict()
    mutator(payload)

    with pytest.raises(ContractError):
        IntervalSchedule.from_dict(payload)


@pytest.mark.parametrize("value", [None, [], "text", 3])
def test_from_dict_requires_an_object_mapping(value: object) -> None:
    with pytest.raises(ContractError, match="object mapping"):
        IntervalSchedule.from_dict(value)  # type: ignore[arg-type]
