import hashlib
from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick


def test_restore_retains_old_schedule_and_requires_new_successor_intent(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    schedules = ScheduleRepository(repo)
    old = schedules.create(IntervalSchedule("old", snapshot.session, snapshot.goal.digest,
                                            100, 100, "scheduled input"))
    backup, prepared, transformed = (tmp_path / n for n in ("backup", "prepared", "new"))
    receipt = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    source_hash = hashlib.sha256(repo.db_path.read_bytes()).hexdigest()
    candidate = transform_runtime_restore(prepared, receipt["sha256"], (snapshot,),
                                          source_hash, transformed)
    activate_runtime_restore(repo, tmp_path / "supervisor.lock", transformed,
                             candidate["sha256"], receipt["sha256"], source_hash)
    current = repo.read_current("session")
    assert current.session == snapshot.session.successor()
    before = repo.db_path.read_bytes()
    assert schedules.read(snapshot.session, "old") == old
    with pytest.raises(ContractError):
        ScheduleTick(repo).tick(old, 100)
    with pytest.raises(ContractError):
        schedules.create(replace(old, session=current.session))
    assert repo.db_path.read_bytes() == before
    fresh = schedules.create(replace(old, schedule_id="successor", session=current.session))
    admitted = ScheduleTick(repo).tick(fresh, 100)
    assert len(admitted["occurrences"]) == 1
    assert ScheduleTick(repo).tick(fresh, 100)["occurrences"] == []
    assert schedules.read(snapshot.session, "old") == old
    with repo._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1
