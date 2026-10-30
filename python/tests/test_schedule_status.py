import hashlib
import json

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_status import inspect_schedule
from ahc.infrastructure.schedule_tick import ScheduleTick


def test_cli_progress_is_readonly_and_distinguishes_admission(tmp_path, capsys):
    store, current, _ = setup(tmp_path)
    value = IntervalSchedule("s", current.session, current.goal.digest, 100, 100, "PRIVATE")
    schedules = ScheduleRepository(store)
    schedules.create(value)
    ScheduleTick(store).tick(value, 1000, limit=3)
    schedules.set_state(value, "paused")
    before = hashlib.sha256(store.db_path.read_bytes()).digest()
    assert main(["schedule-status", "--runtime-root", str(store.root), "--session-json",
                 json.dumps(current.session.to_dict()), "--schedule-id", "s",
                 "--now-ms", "1000"]) == 0
    output = capsys.readouterr().out
    progress = json.loads(output)["progress"]
    assert progress["through_ms"] == 300
    assert progress["overdue_cursor_slots"] == 7
    assert progress["recorded_occurrences"] == 3
    assert progress["next_cursor_due_ms"] == 400
    assert not progress["eligible"] and "PRIVATE" not in output
    assert hashlib.sha256(store.db_path.read_bytes()).digest() == before


def test_absent_foreign_and_corrupt_cursor(tmp_path):
    store, current, _ = setup(tmp_path)
    assert inspect_schedule(store, current.session, "missing", 1) == {"status": "absent"}
    value = IntervalSchedule("s", current.session, current.goal.digest, 100, 100, "hi")
    ScheduleRepository(store).create(value)
    foreign = VirtualSessionIdentity.initial("other", "agent", "route")
    with pytest.raises(ContractError, match="owner"):
        inspect_schedule(store, foreign, "s", 100)
    ScheduleTick(store).tick(value, 100)
    store._mutate(lambda c: c.execute("DELETE FROM runtime_schedule_occurrences"))
    with pytest.raises(ContractError, match="backlink"):
        inspect_schedule(store, current.session, "s", 100)
