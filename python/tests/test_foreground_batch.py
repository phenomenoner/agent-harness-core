import time
from dataclasses import replace

import pytest
from test_foreground_config_v2 import valid_config
from test_runtime_core import setup

from ahc.application.runtime_service import RuntimeService
from ahc.application.tool_batch import BatchCall
from ahc.cli.foreground_batch import compose_batch
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError


def test_explicit_grants_bind_digest_and_preview_without_default_expansion():
    value = valid_config()
    old = parse_foreground_config_v2(value)
    assert "batch" not in old.preview()
    value["batch"] = {"agent_id": old.session.agent_id,
                      "capabilities": ["runtime.result.read"]}
    new = parse_foreground_config_v2(value)
    assert new.configuration_digest != old.configuration_digest
    assert new.preview()["batch"] == value["batch"]


@pytest.mark.parametrize("grant", [
    {"agent_id": "foreign", "capabilities": ["runtime.result.read"]},
    {"agent_id": "agent", "capabilities": ["runtime.submit"]},
    {"agent_id": "agent", "capabilities": []},
])
def test_invalid_grants_reject_without_composition(grant):
    value = valid_config()
    value["batch"] = grant
    with pytest.raises(ContractError):
        parse_foreground_config_v2(value)


def test_batch_requires_active_runtime_turn_and_cannot_outlive_it(tmp_path):
    store, snapshot, request = setup(tmp_path)
    value = valid_config()
    value["session"], value["workspace_id"] = snapshot.session.to_dict(), request.workspace_scope
    value["batch"] = {"agent_id": snapshot.session.agent_id,
                      "capabilities": ["runtime.result.read"]}
    batch = compose_batch(parse_foreground_config_v2(value), store)
    request = replace(request, deadline=time.monotonic() + 10)
    call = BatchCall("one", "runtime.result.read", "runtime", '{"operation_id":"missing"}')
    with pytest.raises(ContractError, match="admitted"):
        batch.run(request.session, (call,), deadline=request.deadline)

    class Backend:
        def invoke(self, bound):
            result = batch.run(bound.session, (call,), deadline=bound.deadline, concurrency=2)
            assert result.receipts[0]["state"] == "success"
            assert result.values == (None,)
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1, "t", "v", "done")

    assert RuntimeService(store, Backend()).execute(snapshot, request).result["accepted"]
    with pytest.raises(ContractError, match="admitted"):
        batch.run(request.session, (replace(call, call_id="after"),), deadline=request.deadline)
