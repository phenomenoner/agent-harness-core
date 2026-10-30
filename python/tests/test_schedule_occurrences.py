from dataclasses import replace

import pytest
from test_runtime_core import Backend, setup

from ahc.application.runtime_service import RuntimeService
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_occurrences import ScheduleOccurrences
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.working_repository import WorkingRepositoryError


def test_occurrence_and_inbox_are_atomic_and_reopen_idempotent(tmp_path):
    store, snapshot, request = setup(tmp_path)
    value = IntervalSchedule("s", snapshot.session, snapshot.goal.digest, 1000, 100, "input")
    ScheduleRepository(store).create(value)
    occurrences = ScheduleOccurrences(store)
    with pytest.raises(WorkingRepositoryError, match="pre-commit"):
        occurrences.enqueue(value, 1000, 1000, fail_before_commit=True)
    with store._read() as connection:
        count = connection.execute(
            "SELECT count(*) FROM runtime_schedule_occurrences"
        ).fetchone()[0]
        assert count == 0
    assert RuntimeInbox(store).next_pending(snapshot.session) is None
    first = occurrences.enqueue(value, 1000, 1100)
    second = ScheduleOccurrences(RuntimeRepository(store.root)).enqueue(value, 1000, 1200)
    assert first["operation_id"] == second["operation_id"]
    assert not first["already_submitted"] and second["already_submitted"]
    assert RuntimeInbox(store).query(snapshot.session, first["operation_id"])["status"] == "pending"
    bound = replace(request, operation_id=first["operation_id"], input_text="input")
    backend = Backend()
    assert RuntimeService(store, backend).execute(snapshot, bound).result["accepted"]
    assert occurrences.enqueue(value, 1000, 1500)["already_submitted"]
    assert RuntimeInbox(store).next_pending(snapshot.session) is None
    assert backend.calls == 1


def test_missing_submission_backlink_is_not_silently_recreated(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    value = IntervalSchedule("s", snapshot.session, snapshot.goal.digest, 1000, 100, "input")
    ScheduleRepository(store).create(value)
    occurrences = ScheduleOccurrences(store)
    receipt = occurrences.enqueue(value, 1000, 1000)
    store._mutate(lambda c: c.execute("DELETE FROM runtime_submissions WHERE operation_id=?",
                                    (receipt["operation_id"],)))
    with pytest.raises(ContractError, match="missing"):
        occurrences.enqueue(value, 1000, 1000)


def test_pause_stale_future_and_off_grid_ticks_do_not_submit(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    value = IntervalSchedule("s", snapshot.session, snapshot.goal.digest, 1000, 100, "input")
    schedules = ScheduleRepository(store)
    schedules.create(value)
    occurrences = ScheduleOccurrences(store)
    for due, now in ((1100, 1000), (1050, 1200)):
        with pytest.raises(ContractError):
            occurrences.enqueue(value, due, now)
    paused = schedules.set_state(value, "paused")
    with pytest.raises(ContractError):
        occurrences.enqueue(value, 1000, 1000)
    with pytest.raises(ContractError):
        occurrences.enqueue(paused, 1000, 1000)
    assert RuntimeInbox(store).next_pending(snapshot.session) is None
