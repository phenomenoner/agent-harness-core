"""Closed foreground catch-up through real process restarts and durable admission."""

import json
import os
import subprocess
import sys
from pathlib import Path

from test_runtime_core import setup

import ahc
from ahc.domain.schedules import IntervalSchedule
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository

CHILD = """
import json, sys
import ahc
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.infrastructure.foreground_schedule import ForegroundSchedule
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
root, steps = json.load(sys.stdin)
store = RuntimeRepository(root)
current = store.read_current('session')
calls = []
class Backend:
    def invoke(self, request):
        calls.append(request.operation_id)
        return AgentTurnObservation(request, 'completed', 'fixture', 1, 1,
                                    'thread', 'turn', 'answer')
tick = ForegroundSchedule(store, clock_ms=lambda: 350)
runtime = ForegroundRuntime(store, current.session,
    LocalRuntimeIngress(RuntimeInbox(store), artifact_digest='a'*64,
        configuration_digest=lambda _: 'b'*64, workspace_scope='w'),
    lambda *a, **kw: Backend(), schedule=tick)
accepted = []
try:
    runtime.start()
    for _ in range(steps):
        result = runtime.step()
        if result is not None:
            assert result.result['accepted']
            accepted.append(result.operation_id)
finally:
    runtime.close()
assert runtime.state == 'closed'
print(json.dumps({'calls': calls, 'accepted': accepted, 'schedule': tick.status,
                  'package': ahc.__file__}))
"""


def test_closed_foreground_catches_up_across_restarts_without_duplicate_turns(tmp_path):
    store, current, _ = setup(tmp_path)
    ScheduleRepository(store).create(
        IntervalSchedule("s", current.session, current.goal.digest, 100, 100, "hi")
    )
    # No foreground runs at 100, 200 or 300. At 350, admit/execute only one
    # occurrence, close completely, then resume the remaining backlog.
    env = {key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ}
    env.update(PYTHONPATH=str(Path(ahc.__file__).parent.parent), PYTHONDONTWRITEBYTECODE="1")
    reports = []
    for steps in (2, 5, 2):
        process = subprocess.run(
            [sys.executable, "-B", "-c", CHILD], input=json.dumps([str(store.root), steps]),
            text=True, capture_output=True, env=env, cwd=tmp_path, timeout=15,
        )
        assert process.returncode == 0, process.stderr
        reports.append(json.loads(process.stdout))
    assert [len(report["calls"]) for report in reports] == [1, 2, 0]
    assert all(report["package"] == ahc.__file__ for report in reports)
    operations = [operation for report in reports for operation in report["calls"]]
    assert len(set(operations)) == 3
    assert all(report["calls"] == report["accepted"] for report in reports)
    assert reports[-1]["schedule"] == "not-due"
    reopened = RuntimeRepository(store.root)
    for operation in operations:
        assert reopened.read_turn(current.session, operation).result["accepted"]
    with reopened._read() as connection:
        due = connection.execute(
            "SELECT due_ms FROM runtime_schedule_occurrences ORDER BY due_ms"
        ).fetchall()
        assert [row[0] for row in due] == [100, 200, 300]
        assert connection.execute("SELECT count(*) FROM runtime_turns").fetchone()[0] == 3
        assert connection.execute(
            "SELECT through_ms FROM runtime_schedule_cursors WHERE schedule_id='s'"
        ).fetchone()[0] == 300
