"""Real process exit at schedule/inbox/backend boundaries; no external provider."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_daily_schedules import fixture_zone, ms
from test_runtime_core import Backend, setup

import ahc
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.daily_schedules import DailySchedule
from ahc.domain.schedules import IntervalSchedule
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick

_CHILD = """
import json, os, sys
import ahc
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick
root, now, phase, expected_package = json.load(sys.stdin)
assert ahc.__file__.startswith(expected_package + os.sep)
print('PACKAGE_IDENTITY_VERIFIED', flush=True)
repo = RuntimeRepository(root)
current = repo.read_current('session')
value = ScheduleRepository(repo).read(current.session, 's')
ScheduleTick(repo).tick(value, now, limit=1)
if phase == 'admission':
    os._exit(23)
request = LocalRuntimeIngress(RuntimeInbox(repo), artifact_digest='a'*64,
    configuration_digest=lambda _: 'b'*64, workspace_scope='w')(current, WorkspaceTools())
if phase == 'begin':
    repo.begin(current, request)
    os._exit(23)
class InterruptedBackend:
    def invoke(self, request):
        os._exit(23)
RuntimeService(repo, InterruptedBackend()).execute(current, request)
"""


@pytest.mark.parametrize("kind", ["interval", "daily"])
@pytest.mark.parametrize("phase", ["admission", "begin", "invoke"])
def test_process_loss_preserves_occurrence_without_replay(tmp_path, kind, phase):
    store, current, _ = setup(tmp_path)
    now = ms("2026-11-01T05:30:00+00:00")
    if kind == "interval":
        value = IntervalSchedule("s", current.session, current.goal.digest, now, 1000, "hi")
    else:
        value = DailySchedule("s", current.session, current.goal.digest, now, "hi",
                              "Fixture/US", 1, 30, fixture_zone())
    ScheduleRepository(store).create(value)
    package = str(Path(ahc.__file__).parent.parent)
    env = dict(os.environ, PYTHONPATH=package)
    child = subprocess.run([sys.executable, "-c", _CHILD],
                           input=json.dumps([str(store.root), now, phase, package]), text=True,
                           capture_output=True, env=env, timeout=15)
    assert child.returncode == 23, child.stderr
    assert child.stdout.strip() == "PACKAGE_IDENTITY_VERIFIED"
    reopened = RuntimeRepository(store.root)
    tick = ScheduleTick(reopened).tick(value, now)
    assert tick["through_ms"] == now and tick["occurrences"] == []
    inbox = RuntimeInbox(reopened)
    backend = Backend()
    latest = reopened.read_current("session")
    if phase == "admission":
        request = LocalRuntimeIngress(inbox, artifact_digest="a" * 64,
            configuration_digest=lambda _: "b" * 64, workspace_scope="w")(
                latest, WorkspaceTools())
        assert RuntimeService(reopened, backend).execute(latest, request).result["accepted"]
        assert backend.calls == 1
    else:
        assert inbox.next_pending(current.session) is None
        recovered = reopened.reconcile(current.session)
        assert len(recovered) == 1
        assert recovered[0].result["disposition"] == "unknown"
        assert recovered[0].result["unresolved_effects"]
        assert backend.calls == 0
    assert inbox.next_pending(current.session) is None
    assert ScheduleTick(reopened).tick(value, now)["occurrences"] == []
    with reopened._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_turns").fetchone()[0] == 1
