import json
from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository


def test_schedule_cli_pause_resume_remove_and_reopen(tmp_path, capsys):
    store, snapshot, _ = setup(tmp_path)
    value = IntervalSchedule("schedule", snapshot.session, snapshot.goal.digest, 1000, 100, "input")
    assert main(["schedule-create", "--runtime-root", str(store.root),
                 "--schedule-json", json.dumps(value.to_dict())]) == 0
    assert json.loads(capsys.readouterr().out)["schedule"] == value.to_dict()
    repo = ScheduleRepository(RuntimeRepository(store.root))
    assert repo.read(snapshot.session, "schedule") == value
    paused = repo.set_state(value, "paused")
    assert paused.next_after(1000) is None
    with pytest.raises(ContractError, match="stale"):
        repo.set_state(value, "removed")
    resumed = repo.set_state(paused, "active")
    assert resumed.next_after(1000) == 1100
    assert main(["schedule-read", "--runtime-root", str(store.root), "--schedule-id", "schedule",
                 "--session-json", json.dumps(snapshot.session.to_dict()),
                 "--after-ms", "1050"]) == 0
    assert json.loads(capsys.readouterr().out)["next_due_ms"] == 1100
    removed = repo.set_state(resumed, "removed")
    reopened = ScheduleRepository(RuntimeRepository(store.root))
    assert reopened.read(snapshot.session, "schedule") == removed
    with pytest.raises(ContractError):
        repo.set_state(removed, "active")
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_turns").fetchone()[0] == 0


def test_owner_goal_and_duplicate_identity_are_not_overwritten(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    value = IntervalSchedule("schedule", snapshot.session, snapshot.goal.digest, 1000, 100, "input")
    repo = ScheduleRepository(store)
    with pytest.raises(ContractError):
        repo.create(replace(value, goal_digest="a" * 64))
    assert repo.create(value) == repo.create(value)
    with pytest.raises(ContractError):
        repo.create(replace(value, interval_ms=200))
    with pytest.raises(ContractError):
        repo.read(VirtualSessionIdentity.initial("other", "agent", "route"), "schedule")
    assert repo.read(snapshot.session, "schedule") == value
