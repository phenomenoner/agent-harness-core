import pytest
from profile_test_support import profile_registry_payload
from skill_test_support import goal

from ahc.application.memory_store_service import MemoryStoreService
from ahc.domain.capability_values import CapabilityGrant
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import digest_json
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.memory_store_approvals import MemoryStoreApprovals
from ahc.infrastructure.runtime_repository import RuntimeRepository


def setup_service(tmp_path, store, reconcile, *, policy="review", grant=True, provider="a" * 64):
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    session = VirtualSessionIdentity.initial("session", "hexi", "route")
    repo.create(WorkingSetSnapshot.initial(session, goal()))
    approvals = MemoryStoreApprovals(repo, clock=lambda: 100)
    kwargs = dict(
        registry=AgentProfileRegistry.from_dict(profile_registry_payload()),
        agent_id="hexi",
        resource_id="hexi.durable-memory",
        scope="hexi",
        provider_digest=provider,
        grants=(CapabilityGrant("hexi", "hexi.durable-memory", "memory.store"),) if grant else (),
        policy=policy,
        approvals=approvals,
        store=store,
        reconcile=reconcile,
    )
    return MemoryStoreService(**kwargs), approvals, session, kwargs


def propose(service, session):
    return service.propose(
        session,
        operation_id="one",
        observation={"kind": "note", "summary": "orchid", "detail": {}},
        origin="user",
        expires_at=200,
    )


def success(**kw):
    original = kw["observation"]
    digest = digest_json({"version": 1, "scope": kw["scope"], "observation": original})
    return {
        "status": "succeeded",
        "record_ref": "opaque",
        "request_digest": digest,
        "record": {
            **original,
            "detail": {**original["detail"], "scope": kw["scope"]},
            "created_at": "now",
        },
        "provenance": {"origin": "unknown"},
        "trust": "untrusted",
        "trace": {"operation_id": kw["operation_id"], "request_digest": digest},
    }


@pytest.mark.parametrize("policy,grant", [("deny", True), ("review", False)])
def test_denied_before_proposal_or_provider(tmp_path, policy, grant):
    def forbidden(**kw):
        pytest.fail("provider called")

    service, _, session, _ = setup_service(
        tmp_path, forbidden, forbidden, policy=policy, grant=grant
    )
    with pytest.raises(ContractError):
        propose(service, session)


def test_approved_exact_send_only_once_and_binding_change_denied(tmp_path):
    calls = []

    def store(**kw):
        calls.append(kw)
        return success(**kw)

    service, approvals, session, kwargs = setup_service(tmp_path, store, lambda **kw: None)
    digest = propose(service, session)
    with pytest.raises(ContractError):
        service.execute(session, digest)
    approvals.approve(session, digest, approval_expires=150)
    wrong = MemoryStoreService(**{**kwargs, "provider_digest": "b" * 64})
    with pytest.raises(ContractError):
        wrong.execute(session, digest)
    assert service.execute(session, digest)["status"] == "succeeded"
    with pytest.raises(ContractError):
        service.execute(session, digest)
    assert len(calls) == 1


def test_response_lost_reconcile_no_resend(tmp_path):
    saved = []

    def store(**kw):
        saved.append(success(**kw))
        raise OSError("private")

    service, approvals, session, _ = setup_service(tmp_path, store, lambda **kw: saved[0])
    digest = propose(service, session)
    approvals.approve(session, digest, approval_expires=150)
    assert service.execute(session, digest) == {"status": "unknown"}
    assert service.reconcile_only(session, digest)["status"] == "succeeded"
    assert len(saved) == 1 and approvals.query(session, digest)["state"] == "succeeded"
    with pytest.raises(ContractError):
        service.execute(session, digest)


def test_wrong_provider_success_is_unknown(tmp_path):
    def store(**kw):
        value = success(**kw)
        value["record"]["summary"] = "other"
        return value

    service, approvals, session, _ = setup_service(tmp_path, store, lambda **kw: None)
    digest = propose(service, session)
    approvals.approve(session, digest, approval_expires=150)
    assert service.execute(session, digest) == {"status": "unknown"}


def test_local_finish_failure_after_provider_success_is_unknown(tmp_path, monkeypatch):
    service, approvals, session, _ = setup_service(tmp_path, success, lambda **kw: None)
    digest = propose(service, session)
    approvals.approve(session, digest, approval_expires=150)

    def lost(*args):
        raise ContractError("local result persistence lost")

    monkeypatch.setattr(approvals, "finish", lost)
    assert service.execute(session, digest) == {"status": "unknown"}
    assert approvals.query(session, digest)["state"] == "in-flight"
