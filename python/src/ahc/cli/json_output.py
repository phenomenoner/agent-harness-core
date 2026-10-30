"""Bounded, explicit JSON output adapter for every CLI command."""

from __future__ import annotations

import json
import math
from typing import Any

MAX_JSON_DEPTH = 64
MAX_JSON_CONTAINER_ENTRIES = 10_000
MAX_JSON_VALUES = 50_000
MAX_JSON_STRING_CHARS = 16_384
MAX_JSON_INTEGER_BITS = 1_024

_FALLBACK_EXIT_CODE = 1
_FALLBACK_OUTPUT = (
    '{"error": {"kind": "serialization", "message": "command output could not be encoded"}, '
    '"exit_code": 1, "ok": false}'
)
_ERROR_MESSAGES = {
    "config": "invalid configuration",
    "contract": "command contract violated",
    "internal": "internal command failure",
    "interrupt": "command interrupted",
    "serialization": "command output could not be encoded",
    "usage": "invalid command usage",
}


class PublicJsonError(ValueError):
    """Raised when a CLI payload is outside the public JSON contract."""


class _ConversionState:
    def __init__(self) -> None:
        self.value_count = 0
        self.active_containers: set[int] = set()


def emit_lifecycle_result(command: str, exit_code: int, report: Any) -> int:
    try:
        safe_exit_code = _exit_code(exit_code)
    except PublicJsonError:
        _emit_fallback()
        return _FALLBACK_EXIT_CODE
    return emit(
        {
            "command": command,
            "exit_code": safe_exit_code,
            "ok": safe_exit_code == 0,
            "report": report,
        }
    )


def emit_error(command: str | None, exit_code: int, message: str, kind: str) -> int:
    """Emit a fixed public error without inspecting caller-supplied error text."""
    _ = message
    safe_kind = kind if type(kind) is str and kind in _ERROR_MESSAGES else "internal"
    payload: dict[str, Any] = {
        "error": {"kind": safe_kind, "message": _ERROR_MESSAGES[safe_kind]},
        "exit_code": exit_code,
        "ok": False,
    }
    if type(command) is str and len(command) <= MAX_JSON_STRING_CHARS:
        payload["command"] = command
    return emit(payload)


def emit_raw(payload: dict[str, Any], exit_code: int) -> int:
    try:
        safe_exit_code = _exit_code(exit_code)
    except PublicJsonError:
        _emit_fallback()
        return _FALLBACK_EXIT_CODE
    return safe_exit_code if _emit_payload(payload) else _FALLBACK_EXIT_CODE


def emit(payload: dict[str, Any]) -> int:
    try:
        safe_exit_code = _payload_exit_code(payload)
    except PublicJsonError:
        _emit_fallback()
        return _FALLBACK_EXIT_CODE
    return safe_exit_code if _emit_payload(payload) else _FALLBACK_EXIT_CODE


def _payload_exit_code(payload: object) -> int:
    if type(payload) is not dict:
        raise PublicJsonError("payload must be an object")
    return _exit_code(payload.get("exit_code"))


def _exit_code(value: object) -> int:
    if type(value) is not int or not 0 <= value <= 255:
        raise PublicJsonError("exit code must be a byte")
    return value


def _emit_payload(payload: object) -> bool:
    try:
        encoded = json.dumps(
            _json_value(payload), ensure_ascii=False, sort_keys=True, allow_nan=False
        )
    except Exception:
        _emit_fallback()
        return False
    print(encoded)
    return True


def _emit_fallback() -> None:
    print(_FALLBACK_OUTPUT)


def _json_value(value: object, state: _ConversionState | None = None, depth: int = 0) -> Any:
    """Return a JSON value without invoking user-defined conversion methods."""
    if depth > MAX_JSON_DEPTH:
        raise PublicJsonError("JSON nesting limit exceeded")
    if state is None:
        state = _ConversionState()
    state.value_count += 1
    if state.value_count > MAX_JSON_VALUES:
        raise PublicJsonError("JSON value limit exceeded")

    value_type = type(value)
    if value is None or value_type is bool:
        return value
    if value_type is str:
        if len(value) > MAX_JSON_STRING_CHARS:
            raise PublicJsonError("JSON string limit exceeded")
        if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
            raise PublicJsonError("JSON string encoding invalid")
        return value
    if value_type is int:
        if value.bit_length() > MAX_JSON_INTEGER_BITS:
            raise PublicJsonError("JSON integer limit exceeded")
        return value
    if value_type is float:
        if not math.isfinite(value):
            raise PublicJsonError("JSON number must be finite")
        return value
    if value_type is dict:
        return _json_object(value, state, depth)
    if value_type is list or value_type is tuple:
        return _json_array(value, state, depth)
    raise PublicJsonError("unsupported public JSON value")


def _json_object(
    value: dict[object, object], state: _ConversionState, depth: int
) -> dict[str, Any]:
    if len(value) > MAX_JSON_CONTAINER_ENTRIES:
        raise PublicJsonError("JSON object entry limit exceeded")
    return _convert_container(
        value,
        state,
        lambda: {
            _json_key(key): _json_value(item, state, depth + 1) for key, item in value.items()
        },
    )


def _json_array(
    value: list[object] | tuple[object, ...], state: _ConversionState, depth: int
) -> list[Any]:
    if len(value) > MAX_JSON_CONTAINER_ENTRIES:
        raise PublicJsonError("JSON array entry limit exceeded")
    return _convert_container(
        value, state, lambda: [_json_value(item, state, depth + 1) for item in value]
    )


def _convert_container(value: object, state: _ConversionState, convert: Any) -> Any:
    identity = id(value)
    if identity in state.active_containers:
        raise PublicJsonError("cyclic JSON value")
    state.active_containers.add(identity)
    try:
        return convert()
    finally:
        state.active_containers.remove(identity)


def _json_key(key: object) -> str:
    if type(key) is not str:
        raise PublicJsonError("JSON object keys must be strings")
    if len(key) > MAX_JSON_STRING_CHARS:
        raise PublicJsonError("JSON string limit exceeded")
    return key


__all__ = [
    "MAX_JSON_CONTAINER_ENTRIES",
    "MAX_JSON_DEPTH",
    "MAX_JSON_INTEGER_BITS",
    "MAX_JSON_STRING_CHARS",
    "MAX_JSON_VALUES",
    "PublicJsonError",
    "emit",
    "emit_error",
    "emit_lifecycle_result",
    "emit_raw",
]
