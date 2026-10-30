from types import SimpleNamespace

import pytest

from ahc.domain import ProcessIdentity
from ahc.infrastructure.process import identity as identities

BOOT = "7964a4ec-a433-48ff-b129-64c23b955af9"


def test_native_identity_survives_wall_clock_drift_but_not_replacement(monkeypatch):
    clock = [100.0]
    generation = [BOOT, 24321115, 4026531836]
    process = SimpleNamespace(pid=42, create_time=lambda: clock[0])
    monkeypatch.setattr(identities, "_LINUX", True)
    monkeypatch.setattr(identities, "_linux_generation", lambda pid: tuple(generation))
    original = identities.identity_for_process(process)
    clock[0] -= 1
    assert identities.identity_matches(original, process)
    assert ProcessIdentity.from_dict(original.to_dict()) == original
    for index, replacement in [(0, "00000000-0000-0000-0000-000000000001"),
                               (1, 24321116), (2, 4026531837)]:
        old = generation[index]
        generation[index] = replacement
        assert not identities.identity_matches(original, process)
        generation[index] = old
    assert not identities.identity_matches(ProcessIdentity(42, 100.0), process)


def test_native_capture_rejects_replacement_between_reads(monkeypatch):
    values = iter([(BOOT, 100, 123), (BOOT, 101, 123)])
    monkeypatch.setattr(identities, "_LINUX", True)
    monkeypatch.setattr(identities, "_linux_generation", lambda pid: next(values))
    with pytest.raises(identities.StaleProcessIdentity):
        identities.identity_for_process(SimpleNamespace(pid=42, create_time=lambda: 100.0))


def test_native_observation_unavailable_is_not_a_match(monkeypatch):
    original = ProcessIdentity(42, 100.0, BOOT, 100, 123)
    monkeypatch.setattr(identities, "_LINUX", True)
    def unavailable(pid):
        raise PermissionError("private")
    monkeypatch.setattr(identities, "_linux_generation", unavailable)
    process = SimpleNamespace(pid=42, create_time=lambda: 100.0)
    assert not identities.identity_matches(original, process)


@pytest.mark.parametrize("changes", [
    {"linux_boot_id": BOOT},
    {"linux_boot_id": BOOT, "linux_start_ticks": 100, "linux_pid_namespace": True},
    {"linux_boot_id": "invalid", "linux_start_ticks": 100, "linux_pid_namespace": 123},
    {"linux_boot_id": BOOT, "linux_start_ticks": -1, "linux_pid_namespace": 123},
])
def test_partial_or_invalid_native_generation_rejected(changes):
    with pytest.raises(ValueError):
        ProcessIdentity.from_dict({"pid": 42, "start_time": 100.0, **changes})


def test_native_identity_crosses_state_and_control_storage(tmp_path):
    from ahc.infrastructure.persistence import ControlStore, StateStore
    owner = ProcessIdentity(42, 100.0, BOOT, 100, 123)
    state = {"schema_version": 1, "status": "running", "supervisor": owner.to_dict(),
             "children": {}, "started_at": 100.0, "updated_at": 101.0}
    StateStore(tmp_path).write(state)
    assert StateStore(tmp_path).read()["supervisor"] == owner.to_dict()
    request = ControlStore(tmp_path).write_stop(owner, request_id="native", requested_at=102.0)
    assert request["target"] == owner.to_dict()
    assert ControlStore(tmp_path).target(ControlStore(tmp_path).read()) == owner
