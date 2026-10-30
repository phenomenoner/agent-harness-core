import base64
import json
import struct
from datetime import datetime

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.domain.daily_schedules import DailySchedule
from ahc.domain.schedule_dispatch import parse_schedule
from ahc.domain.sessions import ContractError
from ahc.infrastructure.foreground_schedule import ForegroundSchedule
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_status import inspect_schedule
from ahc.infrastructure.schedule_tick import ScheduleTick


def ms(value):
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def fixture_zone():
    # Synthetic TZif with the two US-style 2026 transitions, independent of host tzdata.
    transitions = [ms("2026-03-08T07:00:00+00:00") // 1000,
                   ms("2026-11-01T06:00:00+00:00") // 1000]
    raw = b"TZif\0" + bytes(15) + struct.pack(">6i", 0, 0, 0, 2, 2, 8)
    raw += struct.pack(">2i", *transitions) + bytes([1, 0])
    raw += struct.pack(">iBB", -18000, 0, 0) + struct.pack(">iBB", -14400, 1, 4)
    return base64.b64encode(raw + b"STD\0DST\0").decode()


def test_daily_cli_reopen_dst_and_foreground_cursor(tmp_path, capsys):
    store, current, _ = setup(tmp_path)
    start = ms("2026-03-08T00:00:00+00:00")
    due = ms("2026-03-09T06:30:00+00:00")
    value = DailySchedule("daily", current.session, current.goal.digest, start, "hi",
                          "Fixture/US", 2, 30, fixture_zone())
    assert parse_schedule(value.to_dict()) == value
    assert main(["schedule-create", "--runtime-root", str(store.root), "--schedule-json",
                 json.dumps(value.to_dict())]) == 0
    capsys.readouterr()
    reopened = RuntimeRepository(store.root)
    saved = ScheduleRepository(reopened).read(current.session, "daily")
    assert saved == value and saved.next_after(start) == due
    scheduler = ForegroundSchedule(reopened, clock_ms=lambda: due)
    scheduler(current)
    assert scheduler.status == "submitted"
    assert ScheduleTick(reopened).tick(saved, due)["occurrences"] == []
    status = inspect_schedule(reopened, current.session, "daily", due)
    assert status["through_ms"] == due and status["overdue_cursor_slots"] == 0
    paused = ScheduleRepository(reopened).set_state(saved, "paused")
    assert paused.next_after(start) is None
    assert not inspect_schedule(reopened, current.session, "daily", due)["eligible"]


def test_pinned_overlap_and_invalid_timezone_payload(tmp_path):
    _, current, _ = setup(tmp_path)
    start = ms("2026-11-01T00:00:00+00:00")
    args = ("s", current.session, current.goal.digest, start, "hi", "Fixture/US", 1, 30)
    first = DailySchedule(*args, fixture_zone())
    second = DailySchedule(*args, fixture_zone(), overlap="second")
    assert first.next_after(start) == ms("2026-11-01T05:30:00+00:00")
    assert second.next_after(start) == ms("2026-11-01T06:30:00+00:00")
    with pytest.raises(ContractError):
        DailySchedule(*args, base64.b64encode(b"bad").decode())
