"""Recovery branch of the owned stdio probe; server SDK seeds a non-dispatched job."""

import json
import time


def recover(root, store, parent, children, native, envelope, client, binding, expected, grants):
    from ahc.adapters.aar_child_recovery import NativeAarChildRecovery
    from ahc.adapters.aar_native_operations import NativeAarOperations
    from ahc.application.child_dispatch import ChildDispatchService
    from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
    from ahc.infrastructure.child_work_repository import ChildWorkRepository
    from ahc.infrastructure.mcp_receipts import McpReceipts
    from ahc.infrastructure.native_child_repository import NativeChildRepository
    from ahc.infrastructure.runtime_repository import RuntimeRepository

    seed = json.loads((root / "seed.json").read_text())
    operation = NativeAarOperations(client, binding, seed["operation_id"])
    first = operation.status("seed-read", deadline_unix_ms=1010000,
                             deadline=time.monotonic() + 5)
    assert first["status"] == "observed" and first["receipt"].state == "accepted"
    children.admit(envelope)
    children.claim(children.parent, envelope.child_id, "claim")
    identity = {key: expected[key] for key in (
        "capability_digest", "tool_surface_digest", "schema_bundle_digest",
        "catalog_digest", "profile_digest")}
    identity.update(server_id=client.server_id, runtime_generation=binding.runtime_generation,
                    principal_id=binding.principal_id, session_id=binding.session_id,
                    submission_digest=seed["submission_digest"])
    native.reserve(children.parent, envelope.child_id, "claim", identity)
    native.observe(children.parent, envelope.child_id, "claim",
                   json.loads(first["receipt"].payload_json))
    store._mutate(lambda connection: ChildWorkRepository.retire_parent(
        connection, parent.operation_id))
    reopened = ChildDispatchRepository(RuntimeRepository(store.root), parent,
                                       clock=lambda: 1000000)
    retained = NativeChildRepository(reopened)
    before = reopened.read(children.parent)
    recovery = NativeAarChildRecovery(
        retained, children.parent, envelope.child_id, client, binding,
        cancel_grants=(grants["operation.cancel"],))
    result = recovery.cancel("recovery-cancel", {
        **binding.read_context(deadline_unix_ms=1010000),
        "request_id": "recovery-cancel", "idempotency_key": "recovery-cancel-key",
        "grant_id": grants["operation.cancel"], "budget_wall_time_ms": 10000,
    }, deadline=time.monotonic() + 5)
    assert result["status"] == "observed", result
    assert result["receipt"].terminal and result["receipt"].state == "cancelled"
    checked = recovery.status("recovery-read", deadline_unix_ms=1010000,
                              deadline=time.monotonic() + 5)
    assert checked["receipt"].record_payload() == result["receipt"].record_payload()
    assert retained.read(children.parent, envelope.child_id)["receipt"].state == "cancelled"
    assert reopened.read(children.parent) == before

    def no_replay(_):
        raise AssertionError("retired native child replayed")

    local = ChildDispatchService(reopened, no_replay).dispatch(envelope, "new-claim")
    assert local["state"] == "unknown"
    assert int((root / "fixture-calls.txt").read_text()) == 0
    return {"status": "PASS", "scope": "owned stdio recovery cancel; SDK-seeded no-dispatch job",
            "native_state": "cancelled", "child_state": "unknown",
            "model_requests": 0, "provider_requests": 0, "reopen_no_replay": True,
            "mcp_receipts": len(McpReceipts(store, parent).read())}
