from dataclasses import asdict, replace

import pytest
from test_aar_native_operations import Transport
from test_aar_native_route import route_candidate
from test_child_dispatch import state

from ahc.adapters.aar_child_backend import NativeAarChildBackend
from ahc.adapters.aar_native_rlm import NativeAarRlm
from ahc.application.child_dispatch import ChildDispatchService
from ahc.application.mcp_client import McpClient
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.mcp_receipts import McpReceipts
from ahc.infrastructure.native_child_repository import NativeChildRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("mode", ["success", "lost", "bad-trace", "over-budget", "running",
                                 "close-failed", "running-success", "status-lost",
                                 "status-regression", "running-expired", "reconcile"])
@pytest.mark.parametrize("port_kind", ["direct", "foreground"])
def test_native_submit_store_mailbox_and_reopen_no_replay(tmp_path, mode, port_kind, monkeypatch):
    store, parent, clock, children, child = state(tmp_path)
    child = replace(child, execution_kind="minion")
    binding, report, route = route_candidate()
    report["server_now_unix_ms"] = 100
    binding = replace(binding, server_now_unix_ms=100)
    transport = Transport()
    material = {
        "operation": {"type": "operation", "value": "native-op"},
        "strategy": "baseline",
        "answer": "native child answer",
        "steps": [],
        "broker_trace": [],
        "usage": {
            "schema_version": "aar.broker.v1",
            "model_requests": 1,
            "input_tokens": 2,
            "output_tokens": 3,
            "child_operations": 0,
            "artifact_bytes": 0,
        },
    }
    if mode == "over-budget":
        material["usage"]["output_tokens"] = 1000
    result = {
        **material,
        "schema_version": "aar.rlm.v1",
        "trace_digest": "sha256:" + digest_json(material),
    }
    if mode == "bad-trace":
        result["trace_digest"] = "sha256:" + "f" * 64
    pending = mode in {"running", "running-success", "status-lost", "status-regression",
                       "running-expired", "reconcile"}
    transport.payload.update(state="running" if pending else "succeeded",
                             result=None if pending else result)
    if mode == "reconcile":
        transport.payload.update(state="indeterminate", certainty="indeterminate",
                                 reconciliation_required=True)
    transport.fail = mode == "lost"
    native = NativeChildRepository(children)
    created = []
    closes = []
    original_request = transport.request

    def request(method, params, **kwargs):
        if method == "tools/call" and params["name"] == "aar_capabilities":
            transport.calls.append(params)
            return {"content": [], "structuredContent": report}
        if method == "tools/call":
            assert native.read(child.parent, child.child_id)["submission_reserved"]
            if params["name"] == "aar_rlm_execute":
                assert params["arguments"]["start_only"] is True
                assert native.read(child.parent, child.child_id)["binding"][
                    "submission_digest"] == "sha256:" + digest_json(params["arguments"])
            if params["name"] == "aar_operation_status":
                assert params["arguments"]["operation_id"] == "native-op"
                if mode == "running-success":
                    transport.payload.update(state="succeeded", record_revision=2, result=result)
                if mode == "status-regression":
                    transport.payload.update(record_revision=0)
                transport.fail = mode == "status-lost"
        response = original_request(method, params, **kwargs)
        if method == "tools/call" and mode == "running-expired":
            clock[0] = child.deadline_unix_ms
        return response

    def close():
        closes.append(True)
        if mode == "close-failed" and len(closes) == 1:
            raise RuntimeError("fixture close failure")

    transport.request, transport.close = request, close

    def factory(envelope):
        client = McpClient(
            parent.session,
            "native-aar",
            transport,
            authorize=lambda *a: True,
            validate=lambda *a: True,
            clock=lambda: 0,
            receipts=McpReceipts(store, parent),
        )
        client.discover(deadline=0.09)
        rlm = NativeAarRlm(client, binding, **route, approved_grants=("host-model", "host-rlm"))
        context = {
            **binding.read_context(deadline_unix_ms=190),
            "request_id": "child-submit",
            "idempotency_key": "child-submit-key",
            "grant_ids": ["host-model", "host-rlm"],
            "budget_wall_time_ms": 90,
            "budget_model_requests": 1,
            "budget_input_tokens": 1024,
            "budget_output_tokens": 256,
            "budget_child_operations": 0,
            "budget_artifact_bytes": 1024,
        }
        created.append(client)
        if port_kind == "foreground":
            from ahc.cli import native_child_factory
            from ahc.domain.mcp_profiles import McpStdioProfile

            profile = McpStdioProfile("native-aar", parent.session.agent_id,
                (str(tmp_path / "python.exe"),), str(tmp_path), (), (), "a" * 64, True)
            value = {"profile": profile.to_dict(), "pin": asdict(binding.pin),
                     "route": route, "principal_id": binding.principal_id,
                     "session_id": binding.session_id,
                     "grants": {"execute": ["host-model", "host-rlm"]},
                     "budget": {key.removeprefix("budget_"): value
                                for key, value in context.items() if key.startswith("budget_")}}
            def connect(*args, **kwargs):
                assert not kwargs["cancelled"]()
                return client, {}
            monkeypatch.setattr(native_child_factory, "connect_mcp_profile", connect)
            port = native_child_factory.ForegroundNativeChild(
                value, children, envelope, "token", environment={})
            # Keep observation-bound exhaustion deterministic and short in this fixture.
            original_backend = native_child_factory.NativeAarChildBackend
            def backend(*args, **kwargs):
                return original_backend(*args, **kwargs, max_status_reads=2, status_interval=0.001)
            monkeypatch.setattr(native_child_factory, "NativeAarChildBackend", backend)
            return port
        return NativeAarChildBackend(
            envelope, native, rlm, report, context, token="token", deadline=0.09,
            max_status_reads=2, status_interval=0.001,
        )

    service = ChildDispatchService(children, factory)
    record = service.dispatch(child, "token")
    success = mode in {"success", "running-success"}
    assert record["state"] == ("terminal" if success else "unknown")
    if success:
        assert record["result"] == {"output_text": "native child answer"}
    else:
        assert record["result"] is None
    retained = native.read(child.parent, child.child_id)
    assert retained["submission_reserved"]
    assert retained["operation_id"] == (
        None if mode in {"lost", "running-expired"} else "native-op")
    if mode in {"status-lost", "status-regression", "running"}:
        assert retained["receipt"].state == "running"
        assert retained["receipt"].revision == 1
    if mode == "close-failed":
        with pytest.raises(ContractError, match="close remains pending"):
            service.dispatch(child, "new-token")
    service.close()
    assert len(closes) == (2 if mode == "close-failed" else 1)
    assert all(client.state == "closed" for client in created)
    reopened = RuntimeRepository(store.root)
    resumed = ChildDispatchRepository(
        reopened, parent, clock=lambda: clock[0], monotonic_clock=lambda: 0
    )
    again = ChildDispatchService(resumed, lambda _: pytest.fail("must not submit again"))
    assert again.dispatch(child, "new-token") == record
    calls = 3 if mode == "running" else (
        2 if mode in {"running-success", "status-lost", "status-regression"} else 1)
    calls += port_kind == "foreground"
    assert len(transport.calls) == calls
    assert sum(call["name"] == "aar_rlm_execute" for call in transport.calls) == 1
    assert NativeChildRepository(resumed).read(child.parent, child.child_id) == retained
    assert len(McpReceipts(reopened, parent).read()) == calls
