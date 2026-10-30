"""Read-only status and identity-bound operator stop requests."""

from __future__ import annotations

import time
import uuid

from ahc.domain import AppConfig, ProcessIdentity
from ahc.infrastructure.persistence import (
    ControlStore,
    JsonStoreInvalid,
    JsonStoreMissing,
    StateStore,
)
from ahc.infrastructure.process import identity_matches

OK = 0
NOT_RUNNING = 1
INVALID_STATE = 2
LEASE_BUSY = 3
SUPERVISOR_FAILURE = 4
STOP_TIMEOUT = 5
STOP_SUPERSEDED = 6
_ACTIVE_STATES = frozenset({"starting", "running", "stopping", "degraded"})


def inspect_status(config: AppConfig) -> tuple[int, dict[str, object]]:
    """Read status without creating files, repairing state, or signaling a process."""

    code, snapshot = _read_status(config)
    if code == OK:
        from ahc.infrastructure.persistence import AtomicJsonStore
        try:
            readiness = AtomicJsonStore(config.state_dir / "runtime-readiness.json").read()
            admission = readiness.get("admission")
            stamp = readiness.get("updated_at")
            if (readiness.get("supervisor") == snapshot.get("supervisor")
                    and type(stamp) in (int, float) and 0 <= time.time() - stamp <= 30
                    and type(admission) is dict and type(admission.get("ready")) is bool):
                snapshot = {**snapshot, "admission": admission}
        except (JsonStoreMissing, JsonStoreInvalid):
            pass
    return code, snapshot


def request_stop(config: AppConfig, wait_seconds: float) -> tuple[int, dict[str, object]]:
    """Publish a stop request bound to the exact observed supervisor generation."""

    if wait_seconds < 0:
        raise ValueError("wait_seconds must be non-negative")
    code, observation = _read_status(config)
    if code != OK or observation.get("status") not in _ACTIVE_STATES:
        return code, observation
    raw_identity = observation.get("supervisor")
    if not isinstance(raw_identity, dict):
        return INVALID_STATE, {"status": "invalid", "error": "missing supervisor identity"}
    try:
        target = ProcessIdentity.from_dict(raw_identity)
    except ValueError:
        return INVALID_STATE, {"status": "invalid", "error": "invalid supervisor identity"}

    request = ControlStore(config.state_dir).write_stop(
        target,
        request_id=uuid.uuid4().hex,
        requested_at=time.time(),
    )
    if wait_seconds == 0:
        return OK, {**request, "status": "stop-requested"}

    deadline = time.monotonic() + wait_seconds
    while True:
        current_code, current = _read_status(config)
        replacement = _supervisor_identity(current)
        if replacement is not None and replacement != target:
            return STOP_SUPERSEDED, {
                **request,
                "status": "stop-superseded",
                "observed_status": current.get("status"),
                "observed_supervisor": replacement.to_dict(),
            }
        if current.get("status") == "stopped":
            return OK, current
        if current.get("status") in {"stale", "not-started", "invalid"}:
            return current_code, current
        if time.monotonic() >= deadline:
            return STOP_TIMEOUT, {
                **request,
                "status": "stop-timeout",
                "observed_status": current.get("status"),
            }
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))


def _supervisor_identity(observation: dict[str, object]) -> ProcessIdentity | None:
    raw_identity = observation.get("supervisor")
    if not isinstance(raw_identity, dict):
        return None
    try:
        return ProcessIdentity.from_dict(raw_identity)
    except ValueError:
        return None


def _read_status(config: AppConfig) -> tuple[int, dict[str, object]]:
    try:
        snapshot = StateStore(config.state_dir).read()
    except JsonStoreMissing:
        return NOT_RUNNING, {"status": "not-started"}
    except JsonStoreInvalid as exc:
        return INVALID_STATE, {"status": "invalid", "error": str(exc)}

    raw_identity = snapshot.get("supervisor")
    if not isinstance(raw_identity, dict):
        return INVALID_STATE, {"status": "invalid", "error": "missing supervisor identity"}
    try:
        identity = ProcessIdentity.from_dict(raw_identity)
    except ValueError:
        return INVALID_STATE, {"status": "invalid", "error": "invalid supervisor identity"}

    status = snapshot.get("status")
    if status in _ACTIVE_STATES:
        if identity_matches(identity):
            return OK, snapshot
        stale = dict(snapshot)
        stale.update(
            status="stale",
            observed_status=status,
            error="supervisor identity is not alive",
        )
        return NOT_RUNNING, stale
    if status == "stopped":
        return OK, snapshot
    return INVALID_STATE, {"status": "invalid", "error": "unknown lifecycle status"}
