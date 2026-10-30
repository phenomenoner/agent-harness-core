from dataclasses import replace

import pytest
from test_aar_native_binding import candidate
from test_aar_native_operations import Transport
from test_native_child_repository import prepared, receipt

from ahc.adapters.aar_child_recovery import NativeAarChildRecovery
from ahc.application.mcp_client import McpClient
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.native_child_repository import NativeChildRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("mode", ["success", "unknown-id", "generation", "lost", "regression"])
def test_retired_child_reopens_for_exact_status_without_mailbox_revival(tmp_path, mode):
    store, parent, clock, native, identity = prepared(tmp_path)
    pin, report = candidate()
    binding = pin.bind(report, principal_id="host", session_id="native")
    identity.update(runtime_generation=binding.runtime_generation,
                    capability_digest=pin.capability_digest,
                    tool_surface_digest=pin.tool_surface_digest,
                    schema_bundle_digest=pin.schema_bundle_digest)
    native.reserve(parent, "child", "token", identity)
    if mode != "unknown-id":
        native.observe(parent, "child", "token", receipt())
    store._mutate(lambda connection: ChildWorkRepository.retire_parent(
        connection, parent.operation_id))
    clock[0] = 10000
    reopened = NativeChildRepository(
        ChildWorkRepository(RuntimeRepository(store.root), clock=lambda: clock[0]))
    before = reopened.children.read(parent)
    transport = Transport()
    transport.payload = receipt("succeeded", 2)
    transport.fail = mode == "lost"
    if mode == "regression":
        transport.payload = receipt("running", 0)
    client = McpClient(parent.session, "aar", transport,
                       authorize=lambda session, server, tool, digest: (
                           session == parent.session and server == "aar"
                           and tool == "aar_operation_status"),
                       validate=lambda *args: True, clock=lambda: 1)
    client.discover(deadline=10)
    if mode == "generation":
        binding = replace(binding, runtime_generation=binding.runtime_generation + 1)
    try:
        if mode in {"unknown-id", "generation"}:
            with pytest.raises(ContractError):
                NativeAarChildRecovery(reopened, parent, "child", client, binding)
            assert transport.calls == []
        else:
            recovery = NativeAarChildRecovery(reopened, parent, "child", client, binding)
            observed = recovery.status("recovery-status", deadline_unix_ms=2000, deadline=10)
            assert observed["status"] == ("observed" if mode == "success" else "unknown")
            assert reopened.read(parent, "child")["receipt"].state == (
                "succeeded" if mode == "success" else "running")
            assert [call["name"] for call in transport.calls] == ["aar_operation_status"]
        assert reopened.children.read(parent) == before
        assert before[0]["state"] == "unknown"
    finally:
        client.close()


@pytest.mark.parametrize("mismatch", ["binding", "operation", "revision"])
def test_recovery_writer_rechecks_exact_identity_and_preserves_previous(tmp_path, mismatch):
    _, parent, _, native, identity = prepared(tmp_path)
    native.reserve(parent, "child", "token", identity)
    native.observe(parent, "child", "token", receipt("succeeded", 2))
    before = native.read(parent, "child")
    incoming = receipt("succeeded", 3)
    if mismatch == "binding":
        identity = {**identity, "runtime_generation": identity["runtime_generation"] + 1}
    if mismatch == "operation":
        incoming["operation"]["value"] = "other"
    if mismatch == "revision":
        incoming = receipt("running", 1)
    with pytest.raises(ContractError):
        native.observe_recovery(parent, "child", identity, incoming)
    assert native.read(parent, "child") == before


@pytest.mark.parametrize("mode", ["terminal", "indeterminate", "lost", "no-grant",
                                 "wrong-grant", "client-denied", "expired"])
def test_explicit_cancel_and_reconcile_retain_native_truth_without_replay(tmp_path, mode):
    store, parent, _, native, identity = prepared(tmp_path)
    pin, report = candidate()
    binding = pin.bind(report, principal_id="host", session_id="native")
    identity.update(runtime_generation=binding.runtime_generation,
                    capability_digest=pin.capability_digest,
                    tool_surface_digest=pin.tool_surface_digest,
                    schema_bundle_digest=pin.schema_bundle_digest)
    native.reserve(parent, "child", "token", identity)
    native.observe(parent, "child", "token", receipt())
    store._mutate(lambda connection: ChildWorkRepository.retire_parent(
        connection, parent.operation_id))
    child_before = native.children.read(parent)
    transport = Transport()
    transport.payload = receipt("cancelled", 2)
    transport.fail = mode == "lost"
    if mode == "indeterminate":
        transport.payload.update(state="indeterminate", certainty="indeterminate",
                                 reconciliation_required=True)
    client = McpClient(parent.session, "aar", transport,
                       authorize=lambda session, server, tool, digest: (
                           mode != "client-denied" and session == parent.session
                           and server == "aar" and tool in (
                               "aar_operation_cancel", "aar_operation_reconcile")),
                       validate=lambda *args: True, clock=lambda: 1)
    client.discover(deadline=10)
    recovery = NativeAarChildRecovery(
        native, parent, "child", client, binding,
        cancel_grants=() if mode == "no-grant" else ("cancel-grant",),
        reconcile_grants=("reconcile-grant",))
    context = {**binding.read_context(deadline_unix_ms=2000),
               "request_id": "cancel-request", "idempotency_key": "cancel-key",
               "grant_id": "wrong" if mode == "wrong-grant" else "cancel-grant",
               "budget_wall_time_ms": 1000}
    try:
        if mode in {"no-grant", "wrong-grant"}:
            with pytest.raises(ContractError, match="grant not admitted"):
                recovery.cancel("cancel", context, deadline=10)
            assert transport.calls == []
        else:
            result = recovery.cancel("cancel", context, deadline=1 if mode == "expired" else 10)
            expected = "rejected" if mode in {"client-denied", "expired"} else (
                "unknown" if mode == "lost" else "observed")
            assert result["status"] == expected
            if mode == "indeterminate":
                assert not result["receipt"].terminal
                assert native.read(parent, "child")["receipt"].reconciliation_required
                with pytest.raises(ContractError, match="grant not admitted"):
                    recovery.reconcile("wrong-role", context, deadline=10)
                transport.payload = receipt("cancelled", 3)
                result = recovery.reconcile("reconcile", {
                    **context, "grant_id": "reconcile-grant", "request_id": "reconcile-request",
                    "idempotency_key": "reconcile-key"}, deadline=10)
                assert result["receipt"].terminal
            count = len(transport.calls)
            if mode not in {"expired", "client-denied"}:
                assert recovery.cancel("cancel", context, deadline=10)["status"] == "unknown"
            assert len(transport.calls) == count
        expected_state = "cancelled" if mode in {"terminal", "indeterminate"} else "running"
        reopened = NativeChildRepository(ChildWorkRepository(RuntimeRepository(store.root)))
        assert reopened.read(parent, "child")["receipt"].state == expected_state
        assert reopened.children.read(parent) == child_before
        assert all(call["arguments"]["operation_id"] == "native-op" for call in transport.calls)
        assert all(call["name"] != "aar_rlm_execute" for call in transport.calls)
    finally:
        client.close()
