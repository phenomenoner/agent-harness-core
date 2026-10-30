import json

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick
from ahc.infrastructure.working_repository import WorkingRepositoryError


def test_bounded_catchup_reopen_repeat_and_clock_rollback(tmp_path, capsys):
    store, snapshot, _ = setup(tmp_path)
    value = IntervalSchedule("s", snapshot.session, snapshot.goal.digest, 100, 100, "input")
    ScheduleRepository(store).create(value)
    first = ScheduleTick(store).tick(value, 1000, limit=3)
    assert [r["due_ms"] for r in first["occurrences"]] == [100, 200, 300]
    assert first["remaining_due"] == 7
    second = ScheduleTick(RuntimeRepository(store.root)).tick(value, 1000)
    assert len(second["occurrences"]) == 7 and second["remaining_due"] == 0
    assert ScheduleTick(store).tick(value, 50)["through_ms"] == 1000
    assert main(["schedule-tick", "--runtime-root", str(store.root), "--expected-json",
                 json.dumps(value.to_dict()), "--now-ms", "1000"]) == 0
    assert json.loads(capsys.readouterr().out)["tick"]["occurrences"] == []
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 10


def test_tick_rollback_and_pause_reject(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    value = IntervalSchedule("s", snapshot.session, snapshot.goal.digest, 100, 100, "input")
    schedules = ScheduleRepository(store)
    schedules.create(value)
    with pytest.raises(WorkingRepositoryError, match="pre-commit"):
        ScheduleTick(store).tick(value, 1000, fail_before_commit=True)
    with store._read() as connection:
        for table in ("runtime_schedule_cursors", "runtime_schedule_occurrences"):
            assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
    schedules.set_state(value, "paused")
    with pytest.raises(ContractError):
        ScheduleTick(store).tick(value, 1000)
