"""Scheduled foreground admission outcomes and durable no-replay boundaries."""
import json

import pytest
from test_runtime_core import setup

from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.cli import main
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.schedules import IntervalSchedule
from ahc.infrastructure.foreground_schedule import ForegroundSchedule
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick


def test_cli_state_lifecycle_reopens_and_never_dispatches(tmp_path, capsys):
    repo, current, _ = setup(tmp_path)
    value = IntervalSchedule("cli", current.session, current.goal.digest, 100, 100, "task")
    root = ["--runtime-root", str(repo.root)]
    assert main(["schedule-create", *root, "--schedule-json", json.dumps(value.to_dict())]) == 0
    previous = json.loads(capsys.readouterr().out)["schedule"]
    for state, due in (("paused", None), ("active", 200), ("removed", None)):
        assert main(["schedule-state", *root, "--expected-json", json.dumps(previous),
                     "--state", state]) == 0
        changed = json.loads(capsys.readouterr().out)["schedule"]
        assert changed["state"] == state and changed["revision"] == previous["revision"] + 1
        assert main(["schedule-read", *root, "--session-json",
                     json.dumps(current.session.to_dict()),
                     "--schedule-id", "cli", "--after-ms", "100"]) == 0
        readback = json.loads(capsys.readouterr().out)
        assert readback["schedule"] == changed and readback["next_due_ms"] == due
        previous = changed
    with RuntimeRepository(repo.root)._read() as conn:
        assert conn.execute("SELECT count(*) FROM runtime_turns").fetchone()[0] == 0


@pytest.mark.parametrize("mode", ["cancel-before-invoke", "deadline-before-invoke",
                                  "timeout-during-invoke", "cancel-during-invoke"])
def test_scheduled_admitted_outcome_never_replays(tmp_path, monkeypatch, mode):
    repo, current, _ = setup(tmp_path)
    value = IntervalSchedule("s", current.session, current.goal.digest, 100, 100, "task")
    ScheduleRepository(repo).create(value)
    now = [100.0]
    cancelled = [False]
    calls = []
    monkeypatch.setattr("ahc.application.foreground_runtime.time.monotonic", lambda: now[0])

    class Backend:
        def invoke(self, request):
            calls.append("invoke")
            if mode == "timeout-during-invoke":
                raise TimeoutError("private timeout details")
            cancelled[0] = True
            return AgentTurnObservation(request, "unknown", "cancel-unreconciled", 1, 1,
                                        "thread", "turn", unresolved_effects=True)

        def close(self):
            calls.append("close")

    def factory(*args, **kwargs):
        calls.append("factory")
        if mode == "cancel-before-invoke":
            cancelled[0] = True
        if mode == "deadline-before-invoke":
            now[0] = 102.0
        return Backend()

    inbox = RuntimeInbox(repo)
    runtime = ForegroundRuntime(repo, current.session,
        LocalRuntimeIngress(inbox, artifact_digest="a" * 64,
            configuration_digest=lambda _: "b" * 64, workspace_scope="w",
            turn_timeout=1, clock=lambda: now[0]), factory,
        schedule=ForegroundSchedule(repo, clock_ms=lambda: 100))
    runtime.start()
    try:
        assert runtime.step() is None
        pending = inbox.next_pending(current.session)
        assert pending is not None
        result = runtime.step(cancelled=lambda: cancelled[0])
        assert result is not None and result.status == "terminal"
        before_invoke = mode.endswith("before-invoke")
        assert result.result["disposition"] == ("not-submitted" if before_invoke else "unknown")
        assert result.result["unresolved_effects"] is (not before_invoke)
        assert calls.count("invoke") == (0 if before_invoke else 1)
        assert calls.count("factory") == calls.count("close") == 1
    finally:
        runtime.close()
    fresh = RuntimeRepository(repo.root)
    assert RuntimeInbox(fresh).next_pending(current.session) is None
    assert ScheduleTick(fresh).tick(value, 100)["occurrences"] == []
    assert fresh.read_turn(current.session, pending["operation_id"]) == result
    assert "private timeout" not in json.dumps(result.result)
