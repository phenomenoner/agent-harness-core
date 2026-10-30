"""Read-only operator discovery of exact-owner schedules."""

import hashlib
import json

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError
from ahc.infrastructure.schedule_repository import ScheduleRepository


def test_cli_pages_owned_states_without_prompt_or_writes(tmp_path, capsys):
    store, current, _ = setup(tmp_path)
    repository = ScheduleRepository(store)
    for name, state in (("c", "removed"), ("a", "active"), ("b", "paused")):
        item = IntervalSchedule(name, current.session, current.goal.digest, 100, 100, "PRIVATE")
        repository.create(item)
        if state != "active":
            repository.set_state(item, state)
    other = current.session.successor(route_id="other")
    # Read filtering also keeps old generations out of a successor's list.
    assert repository.list_page(other) == ((), None)
    before = hashlib.sha256(store.db_path.read_bytes()).digest()
    base = ["schedule-list", "--runtime-root", str(store.root), "--session-json",
            json.dumps(current.session.to_dict()), "--after-ms", "100", "--limit", "2"]
    assert main(base) == 0
    output = capsys.readouterr().out
    assert "PRIVATE" not in output
    first = json.loads(output)
    assert [x["schedule_id"] for x in first["schedules"]] == ["a", "b"]
    assert [x["next_due_ms"] for x in first["schedules"]] == [200, None]
    assert first["next_after_id"] == "b"
    assert main([*base, "--after-id", first["next_after_id"]]) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["schedules"][0]["state"] == "removed"
    assert second["schedules"][0]["revision"] == 2
    assert second["next_after_id"] is None
    assert hashlib.sha256(store.db_path.read_bytes()).digest() == before


@pytest.mark.parametrize("limit", [0, 101, True])
def test_invalid_page_bound_rejects(tmp_path, limit):
    store, current, _ = setup(tmp_path)
    with pytest.raises(ContractError):
        ScheduleRepository(store).list_page(current.session, limit=limit)
