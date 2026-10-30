import json
from dataclasses import replace

import pytest
from test_backend_custody import Runner, setup

from ahc.domain.child_work import ChildParent, ChildWorkEnvelope
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.child_backend_custody import ChildBackendCustody
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt, WorkingWriterBusy


def envelope(workspace, child_id):
    return ChildWorkEnvelope(child_id, ChildParent(workspace.session, "parent", "a" * 64),
                             "task", workspace.workspace_id, 1000)


def test_parent_and_sibling_child_leases_cleanup_independently(tmp_path):
    repo, parent_custody, parent_lease, workspace, parent_unit = setup(tmp_path)
    first = ChildBackendCustody(repo, envelope(workspace, "first"))
    second = ChildBackendCustody(repo, envelope(workspace, "second"))
    first_lease = first.acquire(workspace.session, workspace.workspace_id)
    second_lease = second.acquire(workspace.session, workspace.workspace_id)
    try:
        assert len({parent_lease.key, first_lease.key, second_lease.key}) == 3
        with pytest.raises(WorkingWriterBusy):
            ChildBackendCustody(repo, envelope(workspace, "first")).acquire(
                workspace.session, workspace.workspace_id)
        first_unit = replace(parent_unit, unit="ahc-backend-" + "c" * 32 + ".service")
        second_unit = replace(parent_unit, unit="ahc-backend-" + "d" * 32 + ".service")
        first.record_created(first_lease, workspace, first_unit)
        second.record_created(second_lease, workspace, second_unit)
        with pytest.raises(ContractError, match="rebound"):
            first.record_created(first_lease, workspace, parent_unit)
        runner = Runner(first_unit)
        assert first.recover(first_lease, runner) == (first_unit,)
        assert runner.stopped
        assert [row[2] for row in parent_custody._held(parent_lease)] == [parent_unit]
        assert [row[2] for row in second._held(second_lease)] == [second_unit]
    finally:
        first_lease.close()
        second_lease.close()
        parent_lease.close()


def test_reopened_child_checks_full_binding_before_cleanup(tmp_path):
    repo, _, parent_lease, workspace, unit = setup(tmp_path)
    parent_lease.close()
    value = envelope(workspace, "child")
    custody = ChildBackendCustody(repo, value)
    lease = custody.acquire(workspace.session, workspace.workspace_id)
    child_unit = replace(unit, unit="ahc-backend-" + "c" * 32 + ".service")
    custody.record_created(lease, workspace, child_unit)
    lease.close()
    reopened = ChildBackendCustody(RuntimeRepository(repo.root), value)
    owner = reopened.acquire(workspace.session, workspace.workspace_id)
    try:
        def corrupt(connection):
            row = connection.execute("SELECT * FROM backend_custody WHERE owner_key=?",
                                     (owner.key,)).fetchone()
            record = json.loads(row["record_json"])
            record["child"]["child_id"] = "other"
            connection.execute("UPDATE backend_custody SET record_json=?,record_digest=? "
                "WHERE owner_key=?",
                (json.dumps(record, sort_keys=True), digest_json(record), owner.key))
        repo._mutate(corrupt)
        runner = Runner(child_unit)
        with pytest.raises(WorkingSetCorrupt):
            reopened.recover(owner, runner)
        assert not runner.calls
    finally:
        owner.close()
