import json
import time
from dataclasses import replace

import pytest
from test_foreground_config_v2 import valid_config
from test_runtime_core import setup

from ahc.application.runtime_service import RuntimeService
from ahc.application.tool_batch import BatchCall
from ahc.cli import main
from ahc.cli.foreground_batch import compose_batch
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.batch_receipt_repository import BatchReceiptRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_reopened_admission_without_receipt_is_unknown_and_cannot_replay(tmp_path):
    store, snapshot, request = setup(tmp_path)
    store.begin(snapshot, request)
    call = BatchCall("one", "runtime.result.read", "runtime", '{"operation_id":"private-input"}')
    BatchReceiptRepository(store).begin(request, (call,))
    reopened = BatchReceiptRepository(RuntimeRepository(store.root))
    rows = reopened.read(request.session, request.operation_id)
    assert rows[0]["state"] == "unknown"
    assert "private-input" not in json.dumps(rows)
    with pytest.raises(ContractError, match="replayed"):
        reopened.begin(request, (replace(call, arguments_json="{}"),))
    with pytest.raises(ContractError, match="owner"):
        other = VirtualSessionIdentity.initial("other", "agent", "route")
        reopened.read(other, request.operation_id)


def test_parallel_turn_receipts_survive_cli_readback(tmp_path, capsys):
    store, snapshot, request = setup(tmp_path)
    request = replace(request, deadline=time.monotonic() + 10)
    config = valid_config()
    config["session"], config["workspace_id"] = snapshot.session.to_dict(), request.workspace_scope
    config["batch"] = {"agent_id": snapshot.session.agent_id,
                       "capabilities": ["runtime.result.read"]}
    batch = compose_batch(parse_foreground_config_v2(config), store)
    calls = tuple(BatchCall(f"item{i}", "runtime.result.read", "runtime",
                            '{"operation_id":"missing"}') for i in range(4))

    class Backend:
        def invoke(self, bound):
            result = batch.run(bound.session, calls, deadline=bound.deadline, concurrency=4)
            assert [r["state"] for r in result.receipts] == ["success"] * 4
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1, "t", "v", "done")

    assert RuntimeService(store, Backend()).execute(snapshot, request).result["accepted"]
    assert main(["runtime-batch-receipts", "--runtime-root", str(store.root),
                 "--session-json", json.dumps(request.session.to_dict()),
                 "--operation-id", request.operation_id]) == 0
    rows = json.loads(capsys.readouterr().out)["receipts"]
    assert [row["state"] for row in rows] == ["success"] * 4
    assert [row["call_id"] for row in rows] == [call.call_id for call in calls]
    assert all("values" not in row for row in rows)
