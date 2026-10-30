"""Foreground protection wiring must refuse a changed pin before process custody."""
from pathlib import Path

import pytest
from test_foreground_workspace_protection import protection

from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.cli.foreground_workspace_protection import native_workspace_protection
from ahc.domain.sessions import ContractError
from ahc.infrastructure.process import linux_native


def test_wrong_isolation_pin_refuses_before_cgroup_or_spawn(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    runtime = tmp_path / "runtime"
    workspace.mkdir()
    runtime.mkdir()
    configured = protection()
    configured.update(
        executable=str(Path("/usr/bin/true").resolve()),
        sha256="0" * 64,
        protected_roots=[str(runtime)],
    )
    options = native_workspace_protection({"cwd": str(workspace), "protection": configured})
    runner = NativeProcessRunner(
        cwd=str(workspace), environment={}, persistent=True, **options
    )

    def forbidden(*args, **kwargs):
        pytest.fail("invalid isolation pin reached cgroup allocation or spawn")

    monkeypatch.setattr(linux_native, "LinuxProcessGroup", forbidden)
    monkeypatch.setattr(linux_native, "spawn_native", forbidden)
    with pytest.raises(ContractError, match="isolation executable changed"):
        runner.prepare_creation(
            (str(Path("/usr/bin/python3").resolve()), "-I", "-c", "pass"),
            lifetime_seconds=5,
        )
    assert runner._group is None
    assert runner._attempt is None
