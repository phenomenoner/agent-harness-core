import pytest
from test_aar_native_operations import setup
from test_aar_native_route import route_candidate

from ahc.adapters.aar_native_rlm import NativeAarRlm
from ahc.domain.sessions import ContractError


def candidate():
    operations, transport = setup()
    binding, report, route = route_candidate()
    # Expose the test tool through ordinary discovery in the shared fixture.
    adapter = NativeAarRlm(operations.client, binding, **route,
                           approved_grants=("host-model", "host-rlm"))
    context = {**binding.read_context(deadline_unix_ms=2000),
               "request_id": "rlm-request", "idempotency_key": "rlm-request-key",
               "grant_ids": ["host-model", "host-rlm"], "budget_wall_time_ms": 1000,
               "budget_model_requests": 1, "budget_input_tokens": 1024,
               "budget_output_tokens": 256, "budget_child_operations": 0,
               "budget_artifact_bytes": 1024}
    return adapter, transport, report, context


def submit(adapter, report, context):
    return adapter.submit("submit", report, context, query="fixture query",
                           strategy="baseline", max_steps=1, deadline=10)


def test_submission_retains_external_operation_not_ahc_call_identity():
    adapter, transport, report, context = candidate()
    result = submit(adapter, report, context)
    assert result["status"] == "observed"
    assert result["operation"].operation_id == "native-op"
    assert result["receipt"].state == "running"
    assert transport.calls[-1]["name"] == "aar_rlm_execute"
    assert transport.calls[-1]["arguments"]["start_only"] is False


@pytest.mark.parametrize("field,value", [
    ("session_id", "wrong"), ("grant_ids", ["not-approved"]),
    ("grant_ids", ["host-rlm", "host-model"]), ("budget_model_requests", 17),
    ("budget_model_requests", True), ("budget_wall_time_ms", 999),
])
def test_rejected_before_send(field, value):
    adapter, transport, report, context = candidate()
    context[field] = value
    with pytest.raises(ContractError):
        submit(adapter, report, context)
    assert not transport.calls


def test_response_loss_never_invents_an_operation_or_retries():
    adapter, transport, report, context = candidate()
    transport.fail = True
    assert submit(adapter, report, context) == {
        "status": "unknown", "operation": None, "receipt": None}
    assert len(transport.calls) == 1
    submit(adapter, report, context)
    assert len(transport.calls) == 1
