from copy import deepcopy

import pytest
from test_aar_native_binding import candidate

from ahc.adapters.aar_native_operations import NativeAarOperations
from ahc.application.mcp_client import PROTOCOL, McpClient
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


class Transport:
    def __init__(self):
        self.calls = []
        self.payload = {"operation": {"type": "operation", "value": "native-op"},
                        "state": "running", "certainty": "certain", "record_revision": 1,
                        "reconciliation_required": False, "result": None,
                        "artifacts": [], "failure": None}
        self.fail = False

    def request(self, method, params, **kwargs):
        if method == "initialize":
            return {"protocolVersion": PROTOCOL, "capabilities": {"tools": {}}}
        if method == "tools/list":
            return {"tools": [{"name": name, "inputSchema": {"type": "object"}}
                    for name in ("aar_operation_status", "aar_operation_cancel",
                                 "aar_operation_reconcile", "aar_artifact_resolve",
                                 "aar_rlm_execute", "aar_capabilities")]}
        self.calls.append(deepcopy(params))
        if self.fail:
            raise TimeoutError("private transport detail")
        return {"content": [], "structuredContent": deepcopy(self.payload)}

    def notify(self, *args, **kwargs):
        pass

    def close(self):
        pass


def setup():
    pin, report = candidate()
    binding = pin.bind(report, principal_id="host", session_id="native-session")
    session = VirtualSessionIdentity.initial("ahc-session", "a", "r")
    transport = Transport()
    client = McpClient(session, "native-aar", transport, authorize=lambda *a: True,
                       validate=lambda *a: True, clock=lambda: 1)
    client.discover(deadline=10)
    return NativeAarOperations(client, binding, "native-op"), transport


def mutation(adapter):
    return {**adapter.binding.read_context(deadline_unix_ms=2000),
            "request_id": "cancel-request", "grant_id": "host-grant",
            "idempotency_key": "cancel-key", "budget_wall_time_ms": 1000}


def test_status_cancel_reconcile_and_terminal_no_regression():
    adapter, transport = setup()
    read = adapter.status("read", deadline_unix_ms=2000, deadline=10)
    assert read["receipt"].state == "running"
    assert transport.calls[-1]["arguments"]["context"]["session_id"] == "native-session"
    transport.payload.update(state="indeterminate", certainty="indeterminate",
                             reconciliation_required=True, record_revision=2)
    cancelled = adapter.cancel("cancel", mutation(adapter), deadline=10)
    assert cancelled["status"] == "observed"
    assert not cancelled["receipt"].terminal
    transport.payload.update(state="succeeded", certainty="certain",
                             reconciliation_required=False, record_revision=3)
    reconciled = adapter.reconcile("reconcile", mutation(adapter), deadline=10)
    assert reconciled["receipt"].terminal
    assert transport.calls[-1]["name"] == "aar_operation_reconcile"
    assert "propose_compensation" not in transport.calls[-1]["arguments"]
    transport.payload.update(state="running", record_revision=4)
    assert adapter.status("regress", deadline_unix_ms=2000, deadline=10)["status"] == "unknown"
    assert adapter.last_receipt == reconciled["receipt"]


@pytest.mark.parametrize("field,value", [
    ("operation", {"type": "operation", "value": "other"}),
    ("state", "complete"), ("record_revision", True),
    ("certainty", "indeterminate"), ("reconciliation_required", None),
])
def test_receipt_mismatch_is_unknown(field, value):
    adapter, transport = setup()
    transport.payload[field] = value
    assert adapter.status("read", deadline_unix_ms=2000, deadline=10) == {
        "status": "unknown", "receipt": None}
    assert adapter.last_receipt is None


def test_lost_cancel_response_does_not_retry_or_claim_cancellation():
    adapter, transport = setup()
    transport.fail = True
    assert adapter.cancel("cancel", mutation(adapter), deadline=10)["status"] == "unknown"
    assert len(transport.calls) == 1
    assert adapter.last_receipt is None
    assert adapter.cancel("cancel", mutation(adapter), deadline=10)["status"] == "unknown"
    assert len(transport.calls) == 1


def test_same_revision_artifact_enrichment_is_not_record_conflict():
    adapter, transport = setup()
    adapter.status("read", deadline_unix_ms=2000, deadline=10)
    transport.payload["artifacts"] = [{"fixture": "receipt-reference"}]
    result = adapter.reconcile("reconcile", mutation(adapter), deadline=10)
    assert result["status"] == "observed"
    # Artifact resolution/validation is separate; this preserves opaque bytes.
    assert 'receipt-reference' in result["receipt"].payload_json
    transport.payload["result"] = "changed record under same revision"
    assert adapter.status("conflict", deadline_unix_ms=2000, deadline=10)["status"] == "unknown"


@pytest.mark.parametrize("field,value", [
    ("session_id", "other"), ("runtime_generation", 8),
    ("grant_id", ""), ("budget_wall_time_ms", 999), ("budget_wall_time_ms", 60001),
])
def test_bad_mutation_context_does_not_dispatch(field, value):
    adapter, transport = setup()
    context = mutation(adapter)
    context[field] = value
    with pytest.raises(ContractError):
        adapter.cancel("cancel", context, deadline=10)
    assert not transport.calls


def test_mutation_budget_may_cover_more_than_remaining_deadline():
    adapter, transport = setup()
    context = mutation(adapter)
    context["budget_wall_time_ms"] = 1001
    assert adapter.cancel("cancel", context, deadline=10)["status"] == "observed"
    assert len(transport.calls) == 1


@pytest.mark.parametrize("lost", [False, True])
def test_cancel_reservation_survives_repository_reopen(tmp_path, lost):
    from test_runtime_core import setup as runtime_setup

    from ahc.infrastructure.mcp_receipts import McpReceipts
    from ahc.infrastructure.runtime_repository import RuntimeRepository

    store, current, request = runtime_setup(tmp_path)
    store.begin(current, request)
    template, transport = setup()
    transport.fail = lost
    # A transport reply with wrong native identity is still a native unknown.
    transport.payload["operation"]["value"] = "wrong-native-operation"

    def compose(repo):
        client = McpClient(current.session, "native-aar", transport,
                           authorize=lambda *a: True, validate=lambda *a: True,
                           clock=lambda: 1, receipts=McpReceipts(repo, request))
        client.discover(deadline=10)
        return NativeAarOperations(client, template.binding, "native-op")

    first = compose(store)
    assert first.cancel("cancel", mutation(first), deadline=10)["status"] == "unknown"
    reopened = RuntimeRepository(store.root)
    second = compose(reopened)
    assert second.cancel("cancel", mutation(second), deadline=10)["status"] == "rejected"
    assert len(transport.calls) == 1
    receipts = McpReceipts(reopened, request).read()
    assert len(receipts) == 1
    assert receipts[0]["status"] == ("unknown" if lost else "observed")
    assert second.last_receipt is None  # MCP receipt is not a native terminal receipt.
