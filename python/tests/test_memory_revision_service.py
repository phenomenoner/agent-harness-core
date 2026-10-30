"""Actual journal corrections through a scoped service; lost replies never resend."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from profile_test_support import profile_registry_payload
from skill_test_support import goal
from test_memory_journal import Embedder, Projection, observation

from ahc.application.memory_revision_service import MemoryRevisionService
from ahc.application.workspace_native_memory import WorkspaceNativeMemory
from ahc.domain.capability_values import CapabilityGrant
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.memory_journal import MemoryJournal
from ahc.infrastructure.runtime_repository import RuntimeRepository


def prepared(tmp_path, *, grant=True, policy="trusted"):
    runtime = RuntimeRepository.initialize(tmp_path / "runtime")
    session = VirtualSessionIdentity.initial("revision-session", "hexi", "route")
    runtime.create(WorkingSetSnapshot.initial(session, goal()))
    journal = MemoryJournal(tmp_path / "journal", embedder=Embedder(),
                            projection=Projection(), create=True)
    original = journal.store(scope="hexi", operation_id="original", observation=observation())
    calls = []

    def revise(**request):
        calls.append(request)
        return journal.revise(**request)

    # A read-only provider fixture. The worker protocol must qualify this method
    # separately; this test does not claim production worker composition.
    def reconcile(*, scope, operation_id, mutation_digest):
        with journal._connect() as conn:
            row = conn.execute(
                "SELECT request_digest,result FROM mutations WHERE scope=? AND operation_id=?",
                (scope, operation_id),
            ).fetchone()
            if row is None or row[0] != mutation_digest:
                return {"status": "unknown"}
            return json.loads(row[1])

    options = dict(
        registry=AgentProfileRegistry.from_dict(profile_registry_payload()),
        agent_id="hexi", resource_id="hexi.durable-memory", scope="hexi",
        provider_digest="a" * 64, policy=policy, repository=runtime,
        grants=(CapabilityGrant("hexi", "hexi.durable-memory", "memory.revise"),)
        if grant else (), revise=revise, reconcile=reconcile,
    )
    return MemoryRevisionService(**options), session, journal, original, calls, options


def correct(service, session, original, **changes):
    request = dict(operation_id="correction", expected_ref=original["record_ref"],
                   observation=observation("Prefer Japanese for this project"))
    return service.revise_trusted(session, **{**request, **changes})


def test_correct_then_tombstone_with_exact_retry_and_original_retention(tmp_path):
    service, session, journal, original, calls, _ = prepared(tmp_path)
    result = correct(service, session, original)
    assert result["status"] == "succeeded"
    assert correct(service, session, original) == result and len(calls) == 1
    assert journal.get(scope="hexi", record_ref=original["record_ref"])["status"] == "not-found"
    assert journal.get(scope="hexi", record_ref=result["record_ref"])["record"]["summary"] == (
        "Prefer Japanese for this project"
    )
    deleted = service.revise_trusted(session, operation_id="withdraw",
                                     expected_ref=result["record_ref"])
    assert deleted["status"] == "succeeded" and deleted["record_ref"] is None
    assert journal.search("language", scope="hexi") == []
    with journal._connect() as conn:
        rows = conn.execute("SELECT retired,observation FROM records ORDER BY id").fetchall()
    assert [row[0] for row in rows] == [1, 1]
    assert json.loads(rows[0][1])["summary"] == observation()["summary"]


@pytest.mark.parametrize("grant,policy", [(False, "trusted"), (True, "deny")])
def test_explicit_grant_and_policy_required_before_dispatch(tmp_path, grant, policy):
    service, session, _, original, calls, _ = prepared(tmp_path, grant=grant, policy=policy)
    with pytest.raises(ContractError):
        correct(service, session, original)
    assert calls == []


def test_lost_reply_reopen_and_reconcile_without_resend(tmp_path):
    service, session, journal, original, calls, options = prepared(tmp_path)
    real = service.revise

    def lost(**request):
        real(**request)
        raise TimeoutError("private provider error")

    service.revise = lost
    result = correct(service, session, original)
    assert result["status"] == "unknown" and len(calls) == 1
    reopened = MemoryRevisionService(**{**options, "repository": RuntimeRepository(
        options["repository"].root)})
    assert correct(reopened, session, original) == result and len(calls) == 1
    recovered = reopened.reconcile_only(session, result["revision_digest"])
    assert recovered["status"] == "succeeded" and len(calls) == 1
    assert journal.get(scope="hexi", record_ref=recovered["record_ref"])["status"] == "succeeded"
    assert correct(reopened, session, original) == recovered


def test_missing_provider_ack_remains_unknown_and_changed_payload_rejects(tmp_path):
    service, session, _, original, calls, _ = prepared(tmp_path)
    service.revise = lambda **kw: {"status": "not-found"}
    result = correct(service, session, original)
    assert service.reconcile_only(session, result["revision_digest"])["status"] == "unknown"
    with pytest.raises(ContractError):
        correct(service, session, original, observation=observation("different"))
    assert calls == []


def test_scope_owner_binding_and_stale_session_rejected(tmp_path):
    service, session, _, original, calls, options = prepared(tmp_path)
    for foreign in (replace(session, agent_id="foreign"), session.successor()):
        with pytest.raises(ContractError):
            correct(service, foreign, original)
    with pytest.raises(ContractError):
        correct(service, session, original,
                observation={**observation(), "detail": {"scope": "bob"}})
    assert calls == []
    result = correct(service, session, original)
    changed = MemoryRevisionService(**{**options, "provider_digest": "b" * 64})
    with pytest.raises(ContractError):
        changed.reconcile_only(session, result["revision_digest"])
    with pytest.raises(ContractError):
        service.reconcile_only(session.successor(), result["revision_digest"])


def test_local_receipt_failure_returns_recoverable_unknown(tmp_path, monkeypatch):
    service, session, _, original, calls, options = prepared(tmp_path)

    def lost(*args):
        raise OSError("receipt unavailable")

    monkeypatch.setattr(service, "_finish", lost)
    result = correct(service, session, original)
    assert result["status"] == "unknown"
    recovered = MemoryRevisionService(**options).reconcile_only(session, result["revision_digest"])
    assert recovered["status"] == "succeeded" and len(calls) == 1


@pytest.mark.parametrize("field,value", [
    ("mutation_digest", "b" * 64), ("previous_ref", "foreign"), ("record_ref", None),
])
def test_mismatched_provider_success_does_not_become_verified(tmp_path, field, value):
    service, session, _, original, _, _ = prepared(tmp_path)
    real = service.revise
    service.revise = lambda **request: {**real(**request), field: value}
    assert correct(service, session, original)["status"] == "unknown"


def test_workspace_surface_exact_shape_and_no_implicit_legacy_extension(tmp_path):
    service, session, _, original, calls, _ = prepared(tmp_path)
    workspace = SimpleNamespace(identity=WorkspaceIdentity(session, "workspace"))
    args = dict(expected=workspace.identity.to_dict(), action="revise", payload=dict(
        operation_id="correction", expected_ref=original["record_ref"],
        observation=observation("new")))
    assert WorkspaceNativeMemory(None, None).invoke(workspace, args)["disposition"] == "rejected"
    native = WorkspaceNativeMemory(None, None, revisions=service)
    bad = {**args, "payload": {**args["payload"], "observation": None}}
    assert native.invoke(workspace, bad)["disposition"] == "rejected"
    wrong = {**args, "expected": WorkspaceIdentity(session, "foreign").to_dict()}
    assert native.invoke(workspace, wrong)["disposition"] == "rejected"
    assert calls == []
    result = native.invoke(workspace, args)
    assert result["disposition"] == "observed" and result["result"]["status"] == "succeeded"
    deleted = native.invoke(workspace, dict(expected=args["expected"], action="tombstone", payload={
        "operation_id": "withdraw", "expected_ref": result["result"]["record_ref"]}))
    assert deleted["result"]["status"] == "succeeded"


def test_duplicate_while_provider_in_flight_never_dispatches_again(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    service, session, _, original, calls, _ = prepared(tmp_path)
    entered, release = Event(), Event()
    real = service.revise

    def held(**request):
        entered.set()
        assert release.wait(5)
        return real(**request)

    service.revise = held
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(correct, service, session, original)
        try:
            assert entered.wait(5)
            duplicate = correct(service, session, original)
            assert duplicate["status"] == "unknown" and calls == []
        finally:
            release.set()
        result = future.result(timeout=5)
    assert result["status"] == "succeeded" and len(calls) == 1
    assert duplicate["revision_digest"] == result["revision_digest"]


def test_closed_workspace_cannot_dispatch_revision(tmp_path):
    service, session, _, original, calls, _ = prepared(tmp_path)
    workspace = SimpleNamespace(identity=WorkspaceIdentity(session, "workspace"), closed=True)
    result = WorkspaceNativeMemory(None, None, revisions=service).invoke(workspace, {
        "expected": workspace.identity.to_dict(), "action": "tombstone",
        "payload": {"operation_id": "withdraw", "expected_ref": original["record_ref"]},
    })
    assert result["disposition"] == "rejected"
    assert calls == []
