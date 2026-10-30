"""Named owners and strict schemas for lifecycle state and control."""

from __future__ import annotations

import math
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from ahc.domain import ProcessIdentity

from .json_store import AtomicJsonStore, JsonStoreError, JsonStoreInvalid, JsonStoreMissing

STATE_FILENAME = "state.json"
CONTROL_FILENAME = "control.json"
_STATE_STATUSES = frozenset({"starting", "running", "stopping", "stopped", "degraded"})
_CHILD_STATUSES = frozenset({"running", "stopped", "failed"})


class StateStore:
    """Atomic lifecycle snapshot repository with strict structural validation."""

    def __init__(self, state_dir: Path | str) -> None:
        self.state_dir = Path(state_dir)
        self._store = AtomicJsonStore(self.state_dir / STATE_FILENAME)

    @property
    def path(self) -> Path:
        return self._store.path

    def write(self, snapshot: Mapping[str, object]) -> None:
        strict = dict(snapshot)
        _validate_state(strict)
        self._store.write(strict)

    def read(self) -> dict[str, object]:
        snapshot = self._store.read()
        _validate_state(snapshot)
        return snapshot

    def exists(self) -> bool:
        return self._store.exists()


class ControlStore:
    """Atomic identity-bound control request repository with a strict schema."""

    def __init__(self, state_dir: Path | str) -> None:
        self.state_dir = Path(state_dir)
        self._store = AtomicJsonStore(self.state_dir / CONTROL_FILENAME)

    @property
    def path(self) -> Path:
        return self._store.path

    def write_stop(
        self,
        target: ProcessIdentity,
        *,
        request_id: str | None = None,
        requested_at: float | None = None,
    ) -> dict[str, object]:
        request_id = request_id or uuid.uuid4().hex
        requested_at = time.time() if requested_at is None else requested_at
        request: dict[str, object] = {
            "schema_version": 1,
            "request_id": request_id,
            "type": "stop",
            "target": target.to_dict(),
            "requested_at": requested_at,
        }
        _validate_control(request)
        self._store.write(request)
        return request

    def read(self) -> dict[str, object]:
        request = self._store.read()
        _validate_control(request)
        return request

    def exists(self) -> bool:
        return self._store.exists()

    @staticmethod
    def target(request: Mapping[str, object]) -> ProcessIdentity:
        raw_target = request.get("target")
        return _identity(raw_target, "control target")


def _validate_state(snapshot: Mapping[str, object]) -> None:
    _exact_keys(
        snapshot,
        required={
            "schema_version",
            "status",
            "supervisor",
            "children",
            "started_at",
            "updated_at",
        },
        optional={"error"},
        label="state snapshot",
    )
    _schema_version(snapshot["schema_version"], "state snapshot")
    status = snapshot["status"]
    if not isinstance(status, str) or status not in _STATE_STATUSES:
        raise JsonStoreInvalid("state snapshot has an invalid status")
    _identity(snapshot["supervisor"], "state supervisor")
    _number(snapshot["started_at"], "state started_at")
    _number(snapshot["updated_at"], "state updated_at")
    error = snapshot.get("error")
    if error is not None and (not isinstance(error, str) or not error):
        raise JsonStoreInvalid("state error must be a non-empty string")
    if status == "degraded" and error is None:
        raise JsonStoreInvalid("degraded state requires an error")

    children = snapshot["children"]
    if not isinstance(children, dict):
        raise JsonStoreInvalid("state children must be an object")
    for name, raw_child in children.items():
        if not isinstance(name, str) or not name:
            raise JsonStoreInvalid("state child names must be non-empty strings")
        _validate_child(name, raw_child)
    if status == "stopped" and any(
        isinstance(child, dict) and child.get("status") == "running" for child in children.values()
    ):
        raise JsonStoreInvalid("stopped state cannot contain a running child")


def _validate_child(name: str, raw_child: object) -> None:
    if not isinstance(raw_child, dict):
        raise JsonStoreInvalid(f"state child {name!r} must be an object")
    _exact_keys(
        raw_child,
        required={"name", "status", "identity", "restarts", "return_code"},
        optional=set(),
        label=f"state child {name!r}",
    )
    if raw_child["name"] != name:
        raise JsonStoreInvalid(f"state child {name!r} has a mismatched name")
    status = raw_child["status"]
    if not isinstance(status, str) or status not in _CHILD_STATUSES:
        raise JsonStoreInvalid(f"state child {name!r} has an invalid status")
    identity = raw_child["identity"]
    if identity is not None:
        _identity(identity, f"state child {name!r} identity")
    if status == "running" and identity is None:
        raise JsonStoreInvalid(f"running state child {name!r} requires an identity")
    restarts = raw_child["restarts"]
    if type(restarts) is not int or restarts < 0:
        raise JsonStoreInvalid(f"state child {name!r} has invalid restarts")
    return_code = raw_child["return_code"]
    if return_code is not None and type(return_code) is not int:
        raise JsonStoreInvalid(f"state child {name!r} has an invalid return code")


def _validate_control(request: Mapping[str, object]) -> None:
    _exact_keys(
        request,
        required={"schema_version", "request_id", "type", "target", "requested_at"},
        optional=set(),
        label="control request",
    )
    _schema_version(request["schema_version"], "control request")
    request_id = request["request_id"]
    if not isinstance(request_id, str) or not request_id:
        raise JsonStoreInvalid("control request_id must be a non-empty string")
    if request["type"] != "stop":
        raise JsonStoreInvalid("control request has an unsupported type")
    _identity(request["target"], "control target")
    _number(request["requested_at"], "control requested_at")


def _identity(value: object, label: str) -> ProcessIdentity:
    if not isinstance(value, dict):
        raise JsonStoreInvalid(f"{label} must be an object")
    _exact_keys(
        value,
        required={"pid", "start_time"},
        optional={"linux_boot_id", "linux_start_ticks", "linux_pid_namespace"},
        label=label,
    )
    try:
        return ProcessIdentity.from_dict(value)
    except ValueError as exc:
        raise JsonStoreInvalid(f"{label} is invalid") from exc


def _schema_version(value: object, label: str) -> None:
    if type(value) is not int or value != 1:
        raise JsonStoreInvalid(f"{label} has an unsupported schema_version")


def _number(value: object, label: str) -> float:
    if type(value) not in (int, float):
        raise JsonStoreInvalid(f"{label} must be a finite non-negative number")
    numeric = float(cast(int | float, value))
    if not math.isfinite(numeric) or numeric < 0:
        raise JsonStoreInvalid(f"{label} must be a finite non-negative number")
    return numeric


def _exact_keys(
    value: Mapping[str, object],
    *,
    required: set[str],
    optional: set[str],
    label: str,
) -> None:
    keys = set(value)
    missing = required - keys
    unknown = keys - required - optional
    if missing:
        raise JsonStoreInvalid(f"{label} is missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise JsonStoreInvalid(f"{label} has unknown fields: {', '.join(sorted(unknown))}")


__all__ = [
    "AtomicJsonStore",
    "CONTROL_FILENAME",
    "ControlStore",
    "JsonStoreError",
    "JsonStoreInvalid",
    "JsonStoreMissing",
    "STATE_FILENAME",
    "StateStore",
]
