import hashlib
import json

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.mcp_receipts import McpReceipts, read_mcp_receipts
from ahc.infrastructure.runtime_health import inspect_runtime_health


def test_receipts_cli_health_are_readonly_and_owner_bound(tmp_path, capsys):
    store, current, request = setup(tmp_path)
    store.begin(current, request)
    receipts = McpReceipts(store, request)
    for call in ("pending", "done"):
        receipts.begin(current.session, call, "server", "a" * 64, "echo", {"secret": "PRIVATE"})
    receipts.finish("done", "observed")
    before = hashlib.sha256(store.db_path.read_bytes()).digest()
    assert main(["runtime-mcp-receipts", "--runtime-root", str(store.root), "--session-json",
                 json.dumps(current.session.to_dict()),
                 "--operation-id", request.operation_id]) == 0
    output = capsys.readouterr().out
    rows = json.loads(output)["receipts"]
    assert rows[0]["status"] == "unknown" and not rows[0]["completion_recorded"]
    assert rows[1]["status"] == "observed" and rows[1]["completion_recorded"]
    assert "PRIVATE" not in output
    health = inspect_runtime_health(store, current.session)["domains"]["mcp_receipts"]
    assert health["state_counts"] == {"completion-unverified": 1, "observed": 1}
    foreign = VirtualSessionIdentity.initial("other", "agent", "route")
    with pytest.raises(ContractError, match="owner"):
        read_mcp_receipts(store, foreign, request.operation_id)
    assert hashlib.sha256(store.db_path.read_bytes()).digest() == before
