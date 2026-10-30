from __future__ import annotations

import pytest

from ahc import ProcessIdentity
from ahc.infrastructure.persistence import (
    AtomicJsonStore,
    ControlStore,
    JsonStoreInvalid,
    JsonStoreMissing,
    StateStore,
)
from ahc.infrastructure.process import InstanceLease, LeaseUnavailable


def test_atomic_json_store_replaces_complete_object(tmp_path) -> None:
    store = AtomicJsonStore(tmp_path / "state.json")
    store.write({"status": "starting", "children": {}})
    store.write({"status": "running", "children": {"one": {"pid": 42}}})
    assert store.read() == {"status": "running", "children": {"one": {"pid": 42}}}
    assert not list(tmp_path.glob(".state.json.*.tmp"))


def test_control_store_is_identity_bound(tmp_path) -> None:
    identity = ProcessIdentity(1234, 5678.25)
    request = ControlStore(tmp_path).write_stop(identity, request_id="r1", requested_at=10.0)
    assert request["type"] == "stop"
    assert set(request) == {"schema_version", "request_id", "type", "target", "requested_at"}
    assert ControlStore(tmp_path).target(ControlStore(tmp_path).read()) == identity


def _valid_state() -> dict[str, object]:
    return {
        "schema_version": 1,
        "status": "running",
        "supervisor": {"pid": 1234, "start_time": 10.0},
        "children": {
            "worker": {
                "name": "worker",
                "status": "running",
                "identity": {"pid": 5678, "start_time": 11.0},
                "restarts": 0,
                "return_code": None,
            }
        },
        "started_at": 10.0,
        "updated_at": 11.0,
    }


@pytest.mark.parametrize(
    "payload",
    [
        {**_valid_state(), "unknown": True},
        {**_valid_state(), "supervisor": {"pid": 1234, "start_time": 10.0, "x": 1}},
        {**_valid_state(), "status": "degraded"},
        {
            **_valid_state(),
            "status": "stopped",
        },
    ],
)
def test_state_store_rejects_unknown_or_inconsistent_structure(tmp_path, payload) -> None:
    AtomicJsonStore(tmp_path / "state.json").write(payload)
    with pytest.raises(JsonStoreInvalid):
        StateStore(tmp_path).read()


@pytest.mark.parametrize(
    "payload",
    [
        {
            "schema_version": 1,
            "request_id": "r1",
            "type": "stop",
            "target": {"pid": 1234, "start_time": 10.0},
            "requested_at": 11.0,
            "action": "stop",
        },
        {
            "schema_version": 1,
            "request_id": "r1",
            "type": "stop",
            "target": {"pid": 1234},
            "requested_at": 11.0,
        },
        {
            "schema_version": 2,
            "request_id": "r1",
            "type": "stop",
            "target": {"pid": 1234, "start_time": 10.0},
            "requested_at": 11.0,
        },
    ],
)
def test_control_store_rejects_unknown_or_malformed_structure(tmp_path, payload) -> None:
    AtomicJsonStore(tmp_path / "control.json").write(payload)
    with pytest.raises(JsonStoreInvalid):
        ControlStore(tmp_path).read()


def test_json_store_distinguishes_missing_and_invalid(tmp_path) -> None:
    store = AtomicJsonStore(tmp_path / "state.json")
    with pytest.raises(JsonStoreMissing):
        store.read()
    store.path.write_text("[]", encoding="utf-8")
    with pytest.raises(JsonStoreInvalid):
        store.read()


@pytest.mark.parametrize(
    "payload",
    [
        '{"status":"running","status":"stopped"}',
        '{"value":NaN}',
    ],
)
def test_json_store_rejects_non_strict_json(tmp_path, payload: str) -> None:
    store = AtomicJsonStore(tmp_path / "state.json")
    store.path.write_text(payload, encoding="utf-8")
    with pytest.raises(JsonStoreInvalid):
        store.read()


def test_json_store_rejects_non_finite_write(tmp_path) -> None:
    store = AtomicJsonStore(tmp_path / "state.json")
    with pytest.raises(JsonStoreInvalid):
        store.write({"value": float("nan")})
    assert not store.path.exists()


def test_single_instance_lease_is_nonblocking(tmp_path) -> None:
    first = InstanceLease(tmp_path / "supervisor.lock")
    second = InstanceLease(tmp_path / "supervisor.lock")
    first.acquire()
    try:
        with pytest.raises(LeaseUnavailable):
            second.acquire()
    finally:
        first.release()
    second.acquire()
    second.release()
