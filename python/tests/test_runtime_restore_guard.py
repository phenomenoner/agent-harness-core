"""Offline source guard composes existing leases without cleaning external IDs."""

import pytest
from test_runtime_core import setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.process.lease import InstanceLease
from ahc.infrastructure.runtime_backup import _digest
from ahc.infrastructure.runtime_restore_guard import offline_restore_source
from ahc.infrastructure.working_db import WORKING_DB_FILENAME
from ahc.infrastructure.working_repository import WorkingWriterBusy
from ahc.infrastructure.workspace_custody import WorkspaceCustody


@pytest.mark.parametrize("owner", [None, "supervisor", "backend", "python-workspace", "workspace",
                                  "pending", "backend-held", "workspace-held", "sidecar", "digest"])
def test_source_guard_retains_ownership_and_rejects_unresolved_state(tmp_path, owner):
    repo, saved, _ = setup(tmp_path)
    backend, workspace = BackendCustody(repo), WorkspaceCustody(repo)
    supervisor = tmp_path / "supervisor.lock"
    lease = None
    if owner == "supervisor":
        lease = InstanceLease(supervisor)
        lease.acquire()
    elif owner == "backend":
        lease = backend.acquire(saved.session, "work")
    elif owner == "python-workspace":
        custody = BackendCustody(repo, resource_role="python-workspace")
        lease = custody.acquire(saved.session, "work")
    elif owner == "workspace":
        lease = workspace.acquire(saved.session, "work")
    elif owner == "pending":
        repo._mutate(lambda c: c.execute("INSERT INTO backend_creation_intents "
                                         "VALUES('unknown','{}','pending')"))
    elif owner in {"backend-held", "workspace-held"}:
        table = "backend_custody" if owner == "backend-held" else "workspace_custody"
        repo._mutate(lambda c: c.execute(
            f"INSERT INTO {table} VALUES('unknown','owner','{{}}','digest','held')"))
    elif owner == "sidecar":
        (repo.root / (WORKING_DB_FILENAME + "-journal")).write_bytes(b"unresolved")
    database = repo.root / WORKING_DB_FILENAME
    before = database.read_bytes()
    digest = "a" * 64 if owner == "digest" else _digest(database)
    try:
        if owner:
            with pytest.raises((ContractError, WorkingWriterBusy)):
                with offline_restore_source(repo, supervisor, digest):
                    pytest.fail("unresolved source admitted")
        else:
            with offline_restore_source(repo, supervisor, digest) as snapshots:
                assert snapshots == (saved,)
                with pytest.raises(WorkingWriterBusy):
                    with repo.execution_lock():
                        pytest.fail("execution owner escaped guard")
    finally:
        if lease is not None:
            lease.release() if owner == "supervisor" else lease.close()
    assert database.read_bytes() == before
    if owner == "sidecar":
        assert (repo.root / (WORKING_DB_FILENAME + "-journal")).read_bytes() == b"unresolved"
