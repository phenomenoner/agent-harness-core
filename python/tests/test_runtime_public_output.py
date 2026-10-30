from __future__ import annotations

import dataclasses
import json
from enum import Enum
from pathlib import Path

import pytest

from ahc.cli.json_output import (
    MAX_JSON_CONTAINER_ENTRIES,
    MAX_JSON_DEPTH,
    MAX_JSON_INTEGER_BITS,
    MAX_JSON_STRING_CHARS,
    PublicJsonError,
    _json_value,
    emit,
    emit_error,
    emit_raw,
)


class _Explosive:
    def __init__(self) -> None:
        self.str_calls = 0
        self.repr_calls = 0

    def __str__(self) -> str:
        self.str_calls += 1
        raise AssertionError("must not stringify public output")

    def __repr__(self) -> str:
        self.repr_calls += 1
        raise AssertionError("must not repr public output")


@dataclasses.dataclass
class _UnexpectedDto:
    value: object

    def __getattribute__(self, name: str) -> object:
        if name == "value":
            raise AssertionError("must not reflect public output")
        return object.__getattribute__(self, name)


class _UnexpectedEnum(Enum):
    VALUE = "value"


def test_emit_accepts_only_explicit_json_dto(capsys) -> None:
    code = emit(
        {
            "exit_code": 0,
            "ok": True,
            "report": {"items": [{"id": 1, "label": "ready"}], "modes": ("safe", "fixed")},
        }
    )

    assert code == 0
    assert json.loads(capsys.readouterr().out) == {
        "exit_code": 0,
        "ok": True,
        "report": {"items": [{"id": 1, "label": "ready"}], "modes": ["safe", "fixed"]},
    }


def test_unexpected_values_are_rejected_without_stringification_or_reflection() -> None:
    explosive = _Explosive()

    for value in (
        explosive,
        {explosive: "value"},
        _UnexpectedDto(explosive),
        Path("operator-private-path"),
        _UnexpectedEnum.VALUE,
    ):
        with pytest.raises(PublicJsonError):
            _json_value(value)

    assert explosive.str_calls == 0
    assert explosive.repr_calls == 0


def test_emit_error_discards_caller_supplied_error_text(capsys) -> None:
    secret = _Explosive()

    code = emit_error("doctor", 2, secret, "config")  # type: ignore[arg-type]

    assert code == 2
    assert json.loads(capsys.readouterr().out) == {
        "command": "doctor",
        "error": {"kind": "config", "message": "invalid configuration"},
        "exit_code": 2,
        "ok": False,
    }
    assert secret.str_calls == 0
    assert secret.repr_calls == 0


def test_overbound_string_is_rejected_before_stdout(capsys) -> None:
    code = emit({"exit_code": 0, "report": "x" * (MAX_JSON_STRING_CHARS + 1)})

    assert code == 1
    assert json.loads(capsys.readouterr().out) == {
        "error": {"kind": "serialization", "message": "command output could not be encoded"},
        "exit_code": 1,
        "ok": False,
    }


def test_nesting_count_and_integer_bounds_reject_before_encoding() -> None:
    nested: list[object] = []
    cursor = nested
    for _ in range(MAX_JSON_DEPTH + 1):
        child: list[object] = []
        cursor.append(child)
        cursor = child

    for value in (
        nested,
        list(range(MAX_JSON_CONTAINER_ENTRIES + 1)),
        1 << MAX_JSON_INTEGER_BITS,
    ):
        with pytest.raises(PublicJsonError):
            _json_value(value)


def test_cycle_and_nonfinite_number_fall_back_to_one_fixed_json_line(capsys) -> None:
    cycle: list[object] = []
    cycle.append(cycle)

    assert emit_raw({"before": "valid", "after": cycle}, 0) == 1
    cycle_output = capsys.readouterr().out.splitlines()
    assert len(cycle_output) == 1
    assert "valid" not in cycle_output[0]
    assert json.loads(cycle_output[0])["error"]["kind"] == "serialization"

    assert emit_raw({"number": float("nan")}, 0) == 1
    nonfinite_output = capsys.readouterr().out.splitlines()
    assert len(nonfinite_output) == 1
    assert json.loads(nonfinite_output[0])["error"]["kind"] == "serialization"


def test_surrogate_is_rejected_before_public_stdout(capsys):
    from ahc.cli.json_output import emit_raw

    assert emit_raw({"value": chr(0xD800)}, 0) == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    assert '"kind": "serialization"' in captured.out
