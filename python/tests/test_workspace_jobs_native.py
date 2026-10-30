"""Disposable native same-job scenarios inside an explicitly delegated user service."""

import sys
import time
from pathlib import Path

import pytest
from test_linux_native import native_owner
from test_runtime_core import setup

from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.adapters.native_python_workspace import NativePythonWorkspace
from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.application.workspace_jobs import WorkspaceJobs
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.runtime_policy import EffectivePolicy

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native Linux cgroup scenario")


@pytest.fixture
def native_work(tmp_path, native_owner):
    store, snapshot, _ = setup(tmp_path)
    directory = tmp_path / "kernel"
    directory.mkdir()
    runner = NativeProcessRunner(cwd=str(directory), environment={},
                                 persistent=True, owner_lifetime=True)
    policy = EffectivePolicy(1, "yolo", "danger-full-access", True, "a" * 64, 30)
    def factory(identity, checkpoint=None):
        return NativePythonWorkspace(store, runner, identity, effective_policy=policy, user=None,
                                  environment_digest="a" * 64, cwd=str(directory),
                                  interpreter=sys.executable, lifetime_seconds=90,
                                  checkpoint=checkpoint)
    workspace = RuntimeWorkspace.open(store, snapshot, "work", factory)
    try:
        yield store, workspace, WorkspaceTools(workspace), directory
    finally:
        workspace.close()


def call(tools, action, job_id="native-job", **fields):
    return tools.invoke("workspace.job", {"expected": tools.workspace.identity.to_dict(),
                         "action": action, "job_id": job_id, **fields})


def wait_started(directory):
    deadline = time.monotonic() + 5
    while not (directory / "started").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert (directory / "started").exists()


def test_native_long_job_yields_and_duplicate_observes_one_execution(native_work):
    _, workspace, tools, _ = native_work
    code = "import time; counter = globals().get('counter', 0) + 1; time.sleep(31.1); print(counter)"
    with workspace.turn_scope(time.monotonic() + 75):
        started = call(tools, "start", code=code, expected_seconds=70, wait_seconds=0)
        assert started["disposition"] == "observed"
        assert started["result"]["status"] == "running"
        waited = call(tools, "wait", wait_seconds=30)
        # The original estimate now exceeds the remaining turn budget; duplicate
        # admission must still observe its receipt instead of constructing a lease.
        duplicate = call(tools, "start", code=code, expected_seconds=70)
        assert duplicate["disposition"] == "observed"
        completed = call(tools, "wait", wait_seconds=10)["result"]
        assert completed["status"] == "complete"
        assert completed["execution"]["output"].strip() == "1"
        assert workspace.execute(workspace.identity, "print(counter)")["output"].strip() == "1"
        workspace.jobs = WorkspaceJobs(workspace)
        assert call(tools, "status")["result"]["status"] == "complete"
        budget = tools.invoke("workspace.budget", {"expected": workspace.identity.to_dict()})
        assert budget["result"]["resources"]["workspace"]["identity_verified"] is True
        observation = budget["result"]["resources"]["workspace"]
        # Delegated services need not expose every ancestor controller file.
        # The native boundary must preserve missing observations as unknown.
        assert observation["hierarchy_complete"] is True
        missing = {key for level in [observation["leaf"], *observation["ancestors"]]
                   for key, value in level["controllers"].items() if value["state"] == "unknown"}
        assert observation["status"] == ("partial" if missing else "observed")
        for key in missing:
            assert observation["effective_limits"][key] == {
                "state": "unknown", "reason": "ancestor-unavailable"}


def test_native_silent_job_cancel_stops_exact_owned_group(native_work):
    _, workspace, tools, directory = native_work
    code = "from pathlib import Path; import time; Path('started').write_text('once'); time.sleep(20)"
    with workspace.turn_scope(time.monotonic() + 40):
        assert call(tools, "start", code=code, expected_seconds=30)["disposition"] == "observed"
        wait_started(directory)
        unit = workspace.port._unit
        started = time.monotonic()
        cancelled = call(tools, "cancel")["result"]
        assert time.monotonic() - started < 3
        assert cancelled["status"] == "cancelled"
        assert cancelled["cancellation"]["execution_stopped"] is True
        assert cancelled["cancellation"]["effects_unknown"] is True
        assert not Path("/sys/fs/cgroup" + unit.cgroup).exists()
        assert call(tools, "status")["result"]["execution"]["execution_stopped"] is True
        assert (directory / "started").read_text() == "once"


def test_native_estimate_expiry_is_unknown_and_never_replayed(native_work):
    _, workspace, tools, directory = native_work
    code = "from pathlib import Path; import time; Path('started').write_text('once'); time.sleep(20)"
    with workspace.turn_scope(time.monotonic() + 40):
        admitted = call(tools, "start", code=code, expected_seconds=0.3)
        assert admitted["disposition"] == "observed"
        unit = workspace.port._unit
        terminal = call(tools, "wait", wait_seconds=5)
        assert terminal["disposition"] == "unknown"
        assert terminal["result"]["replay_allowed"] is False
        assert not Path("/sys/fs/cgroup" + unit.cgroup).exists()
        workspace.jobs = WorkspaceJobs(workspace)
        assert call(tools, "status")["result"]["status"] == "unknown"
        assert call(tools, "start", code=code, expected_seconds=0.3)["disposition"] == "rejected"
        assert (directory / "started").read_text() == "once"
