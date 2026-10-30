from concurrent.futures import ThreadPoolExecutor

import pytest
from test_runtime_core import setup

from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_store_approvals import MemoryStoreApprovals
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingWriterBusy


def prepared(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    approvals = MemoryStoreApprovals(repo, clock=lambda: 100)
    digest = approvals.stage(
        snapshot.session,
        operation_id="remember-one",
        resource_id="agent.memory",
        provider_digest="a" * 64,
        scope="agent",
        observation={"kind": "note", "summary": "orchid", "detail": {}},
        origin="user",
        expires_at=200,
    )
    return repo, snapshot, approvals, digest


def test_review_exact_payload_reopen_consume_once_and_unknown(tmp_path):
    repo, snapshot, owner, digest = prepared(tmp_path)
    assert owner.query(snapshot.session, digest)["proposal"]["observation"]["summary"] == "orchid"
    with pytest.raises(ContractError):
        owner.consume(snapshot.session, digest)
    owner.approve(snapshot.session, digest, approval_expires=150)
    fresh = MemoryStoreApprovals(RuntimeRepository(repo.root), clock=lambda: 110)
    assert fresh.consume(snapshot.session, digest)["provider_digest"] == "a" * 64
    fresh.finish(snapshot.session, digest, "unknown")
    with pytest.raises(ContractError):
        fresh.consume(snapshot.session, digest)
    with pytest.raises(ContractError):
        fresh.approve(snapshot.session, digest, approval_expires=140)
    assert fresh.query(snapshot.session, digest)["state"] == "unknown"


def test_expiry_and_successor_deny_consumption(tmp_path):
    repo, snapshot, owner, digest = prepared(tmp_path)
    owner.approve(snapshot.session, digest, approval_expires=110)
    with pytest.raises(ContractError):
        MemoryStoreApprovals(repo, clock=lambda: 111).consume(snapshot.session, digest)
    WorkingContinuityService(repo).continue_generation(
        snapshot, snapshot.session.successor(), "compaction"
    )
    with pytest.raises(WorkingSetConflict):
        owner.consume(snapshot.session, digest)


def test_concurrent_consumption_has_one_winner(tmp_path):
    repo, snapshot, owner, digest = prepared(tmp_path)
    owner.approve(snapshot.session, digest, approval_expires=150)

    def consume(_):
        try:
            MemoryStoreApprovals(RuntimeRepository(repo.root), clock=lambda: 110).consume(
                snapshot.session, digest
            )
            return True
        except (ContractError, WorkingWriterBusy):
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(consume, range(2))) == [False, True]
