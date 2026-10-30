"""Retirement observations cannot signal a live or replacement resource."""

import os
import sys
from dataclasses import replace

import pytest

from ahc.adapters import native_retirement_observer as module
from ahc.domain.native_process_identity import NativeCreationAttempt, NativeProcessIdentity
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native read-only cgroup adapter")


@pytest.fixture
def recorded(tmp_path, monkeypatch):
    host = tmp_path / "machine-id"
    boot = tmp_path / "boot-id"
    host.write_text("a" * 32)
    boot.write_text("12345678-1234-1234-1234-123456789abc")
    root = tmp_path / "cgroup"
    relative = (
        f"/user.slice/user-{os.geteuid()}.slice/user@{os.geteuid()}.service/"
        "app.slice/ahc-test.service/ahc-process-" + "b" * 32
    )
    group = root / relative.lstrip("/")
    group.mkdir(parents=True)
    (group / "cgroup.events").write_text("populated 0\nfrozen 0\n")
    info = group.stat()
    identity = NativeProcessIdentity(
        host.read_text(),
        boot.read_text(),
        "ahc-test.service",
        "c" * 32,
        relative,
        info.st_dev,
        info.st_ino,
    )
    monkeypatch.setattr(module, "_CGROUP_ROOT", str(root))
    monkeypatch.setattr(module, "_MACHINE_ID", host)
    monkeypatch.setattr(module, "_BOOT_ID", boot)
    return module.NativeRetirementObserver(), identity, group


def test_exact_empty_and_absent_group_are_retired_without_removal(recorded):
    observer, identity, group = recorded
    result = observer.observe(identity)
    assert result.identity == identity and result.state == "stopped" and result.cgroup_empty
    assert group.is_dir()
    (group / "cgroup.events").unlink()
    group.rmdir()
    assert observer.observe(identity).state == "absent"
    assert observer.reconcile_creation(NativeCreationAttempt(identity)) == identity


def test_running_resource_is_observed_but_never_stopped(recorded):
    observer, identity, group = recorded
    (group / "cgroup.events").write_text("populated 1\n")
    assert observer.observe(identity).state == "running"
    with pytest.raises(BackendCustodyUnavailable, match="no stop authority"):
        observer.stop(identity)
    with pytest.raises(BackendCustodyUnavailable, match="remains running"):
        observer.reconcile_creation(NativeCreationAttempt(identity))
    assert (group / "cgroup.events").read_text() == "populated 1\n"


@pytest.mark.parametrize("change", ["host", "boot", "inode", "foreign-user", "events", "symlink"])
def test_unavailable_or_replaced_identity_is_not_absence(recorded, change):
    observer, identity, group = recorded
    if change == "host":
        identity = replace(identity, host_id="d" * 32)
    elif change == "boot":
        identity = replace(identity, boot_id="22345678-1234-1234-1234-123456789abc")
    elif change == "inode":
        identity = replace(identity, inode=identity.inode + 1)
    elif change == "foreign-user":
        identity = replace(
            identity,
            cgroup=identity.cgroup.replace(f"user-{os.geteuid()}.slice", "user-99999.slice"),
        )
    elif change == "events":
        (group / "cgroup.events").write_text("populated unknown\n")
    else:
        (group / "cgroup.events").unlink()
        replacement = group.parent / "replacement"
        replacement.mkdir()
        group.rmdir()
        group.symlink_to(replacement, target_is_directory=True)
    with pytest.raises(BackendCustodyUnavailable, match="unverified"):
        observer.observe(identity)
