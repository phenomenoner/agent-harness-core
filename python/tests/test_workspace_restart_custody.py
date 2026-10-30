"""Durable custody provenance and live-owner exclusion in the existing SQLite."""

import pytest
from skill_test_support import goal

from ahc.adapters.docker_workspace import DockerReply, OwnedContainer, WorkspaceUnavailable
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt, WorkingWriterBusy
from ahc.infrastructure.workspace_custody import WorkspaceCustody


def setup(tmp_path, monkeypatch):
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = repo.create(
        WorkingSetSnapshot.initial(
            VirtualSessionIdentity.initial("session", "agent", "route"), goal()
        )
    )
    identity = repo.reserve_workspace_generation(snapshot, "work")
    custody = WorkspaceCustody(repo)
    lease = custody.acquire(snapshot.session, "work")
    monkeypatch.setattr(OwnedContainer, "verify", lambda self: None)
    owned = OwnedContainer.create(lambda args: DockerReply(0, "a" * 64), ("python", "fixture"))
    custody.record_created(lease, identity, owned)
    return repo, custody, lease, identity


def test_idle_live_owner_blocks_recovery_lease(tmp_path, monkeypatch):
    repo, custody, lease, identity = setup(tmp_path, monkeypatch)
    other = WorkspaceCustody(RuntimeRepository(repo.root))
    try:
        with pytest.raises(WorkingWriterBusy):
            other.acquire(identity.session, identity.workspace_id)
        assert len(custody.held(lease)) == 1
    finally:
        lease.close()


def test_reopen_removes_only_held_full_id_then_is_idempotent(tmp_path, monkeypatch):
    repo, custody, lease, identity = setup(tmp_path, monkeypatch)
    lease.close()
    custody = WorkspaceCustody(RuntimeRepository(repo.root))
    lease = custody.acquire(identity.session, identity.workspace_id)
    calls = []
    present = True

    def runner(args):
        nonlocal present
        calls.append(args)
        if args[0] == "rm":
            assert args == ["rm", "--force", "a" * 64]
            present = False
        return DockerReply(0, "a" * 64 if present else "")

    try:
        assert custody.recover(lease, runner) == ("a" * 64,)
        assert custody.held(lease) == ()
        count = len(calls)
        assert custody.recover(lease, runner) == () and len(calls) == count
    finally:
        lease.close()


def test_corrupt_record_never_calls_docker(tmp_path, monkeypatch):
    repo, custody, lease, identity = setup(tmp_path, monkeypatch)
    repo._mutate(
        lambda connection: connection.execute(
            "UPDATE workspace_custody SET record_digest=?", ("b" * 64,)
        )
    )
    try:
        with pytest.raises(WorkingSetCorrupt):
            custody.recover(lease, lambda args: pytest.fail("Docker called for corrupt custody"))
    finally:
        lease.close()


def test_released_lease_and_arbitrary_identifier_cannot_grant_custody(tmp_path, monkeypatch):
    repo, custody, lease, identity = setup(tmp_path, monkeypatch)
    with pytest.raises(ContractError):
        custody.record_created(lease, identity, "b" * 64)
    lease.close()
    with pytest.raises(ContractError):
        custody.recover(lease, lambda args: pytest.fail("released lease used"))


def test_unverified_removal_keeps_durable_record(tmp_path, monkeypatch):
    repo, custody, lease, identity = setup(tmp_path, monkeypatch)
    calls = []

    def runner(args):
        calls.append(args)
        return DockerReply(0, "a" * 64)

    try:
        with pytest.raises(WorkspaceUnavailable, match="absence"):
            custody.recover(lease, runner)
        assert len(custody.held(lease)) == 1
        assert sum(args[0] == "rm" for args in calls) == 1
    finally:
        lease.close()


def test_failed_port_cleanup_keeps_lifetime_lease():
    from ahc.application.custodied_workspace import CustodiedPort

    class Lease:
        def close(self):
            pytest.fail("lease released before cleanup success")

    class Port:
        def close(self):
            raise WorkspaceUnavailable("cleanup unresolved")

    with pytest.raises(WorkspaceUnavailable):
        CustodiedPort(Port(), Lease()).close()
