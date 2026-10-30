"""Disposable generated user service: real CLI/workbench, crash/restart, exact stop."""

import json
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest
from profile_test_support import profile_registry_payload
from skill_test_support import goal
from test_foreground_config_native import native_config

from ahc.cli.linux_service import render_user_service
from ahc.cli.profile_lifecycle import profile_config
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.config import load_config
from ahc.infrastructure.runtime_repository import RuntimeRepository

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="systemd user service")


def control(*args, check=True):
    return subprocess.run(("/usr/bin/systemctl", "--user", *args), capture_output=True,
                          text=True, timeout=35, check=check)


@pytest.mark.parametrize("resource_partition", [False, True])
def test_generated_service_real_workspace_crash_restart_and_stop(tmp_path, resource_partition):
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    session = VirtualSessionIdentity.initial("session", "agent", "route")
    store.create(WorkingSetSnapshot.initial(session, goal()))
    document = native_config()
    document.update(runtime_root=str(store.root), session=session.to_dict())
    document["workspace"].update(cwd=str(tmp_path), interpreter=sys.executable, lifetime_mode="foreground")
    document["backend"]["enabled"] = True  # No inbox tasks, model execution or credentials.
    (tmp_path / "foreground.json").write_text(json.dumps(document))
    (tmp_path / "profiles.json").write_text(json.dumps(profile_registry_payload(("agent",))))
    config = tmp_path / "app.toml"
    config.write_text('config_version="ahc.standalone.config.v2"\nprofile_registry="profiles.json"\n'
                      'state_dir="lifecycle"\nservices=[]\n[foreground_configs]\nagent="foreground.json"\n')
    secret = tmp_path / "empty.env"
    secret.write_text("")
    unit = "ahc-native-service-" + uuid4().hex + ".service"
    file = tmp_path / unit
    file.write_text(render_user_service(interpreter=sys.executable, config=str(config), agent="agent",
                                        workspace=str(tmp_path), credential_file=str(secret),
                                        resource_partition=resource_partition))
    checked = subprocess.run(("/usr/bin/systemd-analyze", "--user", "verify", str(file)),
                              capture_output=True, text=True, timeout=15)
    assert checked.returncode == 0, checked.stderr
    selected = profile_config(load_config(config), "agent")
    state_file = selected.state_dir / "state.json"
    readiness_file = selected.state_dir / "runtime-readiness.json"

    def wait_ready(previous=None):
        # One fixed budget per start/restart: <=80 observations, <=25 seconds.
        deadline = time.monotonic() + 25
        for _ in range(80):
            try:
                state = json.loads(state_file.read_text())
                ready = json.loads(readiness_file.read_text())
                if (state["status"] == "running" and state["supervisor"] != previous
                        and ready["supervisor"] == state["supervisor"] and ready["admission"]["ready"]):
                    return state
            except (OSError, ValueError, KeyError):
                pass
            if time.monotonic() >= deadline:
                break
            time.sleep(.25)
        pytest.fail("native service readiness deadline; " + control("show", unit,
                    "--property=ActiveState,SubState,Result,ExecMainStatus", check=False).stdout)

    linked = False
    try:
        control("link", str(file))
        linked = True
        control("start", unit)
        first = wait_ready()
        invocation = control("show", unit, "--property=InvocationID", "--value").stdout.strip()
        group = control("show", unit, "--property=ControlGroup", "--value").stdout.strip()
        if resource_partition:
            service_root = Path("/sys/fs/cgroup" + group)
            assert not (service_root / "cgroup.procs").read_text().strip()
            assert {"cpu", "memory", "pids"}.issubset(
                (service_root / "cgroup.subtree_control").read_text().split())
            assert (service_root / "ahc-controller" / "cgroup.procs").read_text().strip()
        owned = list(Path("/sys/fs/cgroup" + group).glob("ahc-process-*"))
        assert len(owned) == 1 and "populated 1" in (owned[0] / "cgroup.events").read_text()
        competing = subprocess.run((sys.executable, "-I", "-m", "ahc", "run", "--config", str(config),
                                    "--agent", "agent", "--max-runtime", "0"),
                                   capture_output=True, text=True, timeout=10)
        assert competing.returncode == 3
        control("kill", "--kill-whom=main", "--signal=SIGKILL", unit)
        second = wait_ready(first["supervisor"])
        assert control("show", unit, "--property=InvocationID", "--value").stdout.strip() != invocation
        assert second["supervisor"] != first["supervisor"] and not owned[0].exists()
        status = subprocess.run((sys.executable, "-I", "-m", "ahc", "status", "--config", str(config),
                                 "--agent", "agent"), capture_output=True, text=True, timeout=10)
        assert status.returncode == 0 and json.loads(status.stdout)["ok"]
        stopped = subprocess.run((sys.executable, "-I", "-m", "ahc", "stop", "--config", str(config),
                                  "--agent", "agent", "--wait-seconds", "10"),
                                 capture_output=True, text=True, timeout=15)
        assert stopped.returncode == 0, stopped.stdout + stopped.stderr
        control("stop", unit)
        assert not Path("/sys/fs/cgroup" + group).exists()
        with store._read() as connection:
            assert connection.execute("SELECT count(*) FROM backend_custody WHERE state='held'").fetchone()[0] == 0
    finally:
        if linked:
            control("stop", unit, check=False)
            control("disable", unit)
            control("reset-failed", unit, check=False)
