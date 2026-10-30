import pytest
from test_mcp_client import Transport
from test_runtime_core import setup

from ahc.application.mcp_client import McpClient
from ahc.domain.sessions import ContractError
from ahc.infrastructure.mcp_receipts import McpReceipts
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("fail", [False, True])
def test_reopen_cannot_repeat_mcp_call(tmp_path, fail):
    store, current, request = setup(tmp_path)
    store.begin(current, request)
    transport = Transport()
    transport.fail = fail

    def client(repo):
        return McpClient(current.session, "server", transport, authorize=lambda *a: True,
            validate=lambda *a: True, clock=lambda: 1, receipts=McpReceipts(repo, request))

    first = client(store)
    first.discover(deadline=10)
    result = first.invoke(current.session, "call", "echo", {"text": "PRIVATE"}, deadline=10)
    assert result["status"] == ("unknown" if fail else "observed")
    reopened = RuntimeRepository(store.root)
    second = client(reopened)
    second.discover(deadline=10)
    assert second.invoke(current.session, "call", "echo", {"text": "changed"},
                         deadline=10)["reason"] == "durable-admission"
    assert transport.calls.count("tools/call") == 1
    receipts = McpReceipts(reopened, request).read()
    assert len(receipts) == 1 and "PRIVATE" not in str(receipts)
    assert receipts[0]["completion_recorded"]


def test_unfinished_reservation_is_unknown_and_immutable(tmp_path):
    store, current, request = setup(tmp_path)
    store.begin(current, request)
    receipts = McpReceipts(store, request)
    receipts.begin(current.session, "call", "server", "a" * 64, "echo", {})
    reopened = McpReceipts(RuntimeRepository(store.root), request)
    assert reopened.read()[0]["status"] == "unknown"
    assert not reopened.read()[0]["completion_recorded"]
    with pytest.raises(ContractError, match="replayed"):
        reopened.begin(current.session, "call", "server", "a" * 64, "echo", {})
    reopened.finish("call", "unknown")
    with pytest.raises(ContractError):
        reopened.finish("call", "observed")
