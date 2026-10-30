from contextlib import nullcontext
from dataclasses import asdict, replace

import pytest
from test_backend_custody import Runner, held_count, setup
from test_wsl_unit_runner import IDENTITY

from ahc.adapters import linux_unit_helper as helper
from ahc.adapters.wsl_unit_runner import WslUnitRunner
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable, UnitRetirement

NEW_BOOT = "87654321-1234-1234-1234-123456789abc"


@pytest.mark.parametrize("key", ["distribution", "boot_id", "unit", "invocation_id"])
def test_malformed_birth_values_fail_closed(key):
    with pytest.raises(helper.ControlError):
        helper._birth_identity({**asdict(IDENTITY), key: None})


@pytest.mark.parametrize("persist_failure", [False, True, "reservation"])
def test_creation_returns_identity_only_after_birth_persistence(
    tmp_path, monkeypatch, persist_failure
):
    monkeypatch.setattr(helper, "ROOT", tmp_path)
    monkeypatch.setattr(helper, "_guard", lambda _: nullcontext())
    monkeypatch.setattr(helper.os, "O_NOFOLLOW", getattr(helper.os, "O_NOFOLLOW", 0), raising=False)
    running = {**asdict(IDENTITY), "state": "running", "cgroup_empty": False}
    monkeypatch.setattr(
        helper,
        "_observe",
        lambda _: {**running, "state": "absent", "invocation_id": None, "cgroup_empty": True},
    )
    calls = []

    def sync_root():
        calls.append("reservation")
        assert (tmp_path / (IDENTITY.unit + ".created")).is_file()
        if persist_failure == "reservation":
            raise OSError("reservation persistence failed")

    monkeypatch.setattr(helper, "_sync_root", sync_root)

    def launch(arguments, unit):
        assert "--expand-environment=no" in arguments[: arguments.index("--")]
        assert arguments[arguments.index("--") + 1 :] == ["/bin/echo", "$HOME", "${AHC_LITERAL}"]
        calls.append("launch")
        return running

    def save(unit, value):
        assert unit == IDENTITY.unit and value == running
        calls.append("persist")
        if persist_failure:
            raise OSError("birth persistence failed")

    monkeypatch.setattr(helper, "_save_birth", save)
    request = {
        "action": "create",
        "unit": IDENTITY.unit,
        "command": ["/bin/echo", "$HOME", "${AHC_LITERAL}"],
        "lifetime_seconds": 1,
    }
    if persist_failure:
        with pytest.raises(OSError):
            helper.control(request, launch=launch)
    else:
        assert helper.control(request, launch=launch) == running
    assert calls == (
        ["reservation"]
        if persist_failure == "reservation"
        else ["reservation", "launch", "persist"]
    )
    assert (tmp_path / (IDENTITY.unit + ".created")).exists()


@pytest.mark.parametrize("changed", [None, "identity", "boot", "replacement"])
def test_retirement_never_stops_and_requires_matching_final_proof(tmp_path, changed):
    repo, custody, lease, _, unit = setup(tmp_path)

    class Retiring(Runner):
        def observe(self, identity):
            value = super().observe(identity)
            if changed == "replacement" and self.calls.count("observe") == 2:
                return replace(value, boot_id=NEW_BOOT, invocation_id="c" * 32)
            return replace(
                value, boot_id=NEW_BOOT, state="absent", invocation_id=None, cgroup_empty=True
            )

        def confirm_retirement(self, identity):
            self.calls.append("retire")
            return UnitRetirement(
                replace(identity, invocation_id="c" * 32) if changed == "identity" else identity,
                "11111111-1234-1234-1234-123456789abc" if changed == "boot" else NEW_BOOT,
            )

    runner = Retiring(unit)
    try:
        if changed is None:
            assert custody.recover(lease, runner) == (unit,)
            assert held_count(repo) == 0
            assert runner.calls == ["observe", "retire", "observe", "retire"]
        else:
            with pytest.raises(BackendCustodyUnavailable):
                custody.recover(lease, runner)
            assert held_count(repo) == 1
        assert "stop" not in runner.calls
    finally:
        lease.close()


@pytest.mark.parametrize(
    "change",
    [
        {},
        {"current_boot_id": IDENTITY.boot_id},
        {"identity": {**asdict(IDENTITY), "invocation_id": "c" * 32}},
        {"extra": True},
    ],
)
def test_wire_retirement_matches_requested_full_identity(change):
    runner = WslUnitRunner("C:/Windows/System32/wsl.exe", IDENTITY.distribution)
    value = {"identity": asdict(IDENTITY), "current_boot_id": NEW_BOOT, **change}
    request = {
        "action": "retire",
        "unit": IDENTITY.unit,
        "boot_id": IDENTITY.boot_id,
        "invocation_id": IDENTITY.invocation_id,
    }
    if not change:
        assert runner._decode({"ok": True, "result": value}, request) == UnitRetirement(
            IDENTITY, NEW_BOOT
        )
    else:
        with pytest.raises(BackendCustodyUnavailable):
            runner._decode({"ok": True, "result": value}, request)


@pytest.mark.parametrize("mutation", [None, "same-boot", "present", "cgroup", "birth", "missing"])
def test_root_retirement_checks_birth_and_absence_without_system_mutation(
    tmp_path, monkeypatch, mutation
):
    monkeypatch.setattr(helper, "ROOT", tmp_path)
    (tmp_path / (IDENTITY.unit + ".created")).write_text("reserved")
    monkeypatch.setattr(helper, "_guard", lambda _: nullcontext())
    value = {
        **asdict(IDENTITY),
        "boot_id": NEW_BOOT,
        "state": "absent",
        "invocation_id": None,
        "cgroup_empty": True,
    }
    if mutation == "same-boot":
        value["boot_id"] = IDENTITY.boot_id
    if mutation == "present":
        value.update(state="running", invocation_id="c" * 32)
    if mutation == "cgroup":
        value["cgroup_empty"] = False
    monkeypatch.setattr(helper, "_observe", lambda _: value)

    def birth(_):
        if mutation == "missing":
            raise OSError("birth missing")
        return {**asdict(IDENTITY), **({"invocation_id": "c" * 32} if mutation == "birth" else {})}

    monkeypatch.setattr(helper, "_read_birth", birth)
    monkeypatch.setattr(helper, "_run", lambda _: pytest.fail("system mutation"))
    request = {
        "action": "retire",
        "unit": IDENTITY.unit,
        "boot_id": IDENTITY.boot_id,
        "invocation_id": IDENTITY.invocation_id,
    }
    if mutation is None:
        assert helper.control(request) == {
            "identity": asdict(IDENTITY),
            "current_boot_id": NEW_BOOT,
        }
    else:
        with pytest.raises((helper.ControlError, OSError)):
            helper.control(request)
