import pytest

from ahc.application.mcp_client import PROTOCOL, McpClient
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


class Transport:
    def __init__(self):
        self.calls = []
        self.fail = False

    def request(self, method, params, **kwargs):
        self.calls.append(method)
        if method == "initialize":
            return {"protocolVersion": PROTOCOL, "capabilities": {"tools": {}}}
        if method == "tools/list":
            return {"tools": [{"name": "echo", "inputSchema": {"type": "object"}}]}
        if self.fail:
            raise TimeoutError("PRIVATE")
        return {"content": [{"type": "text", "text": params["arguments"]["text"]}]}

    def notify(self, method, params, **kwargs):
        self.calls.append(method)

    def close(self):
        self.calls.append("close")


def client(transport, *, allowed=True):
    session = VirtualSessionIdentity.initial("s", "a", "r")
    return McpClient(session, "server", transport, authorize=lambda *a: allowed,
        validate=lambda schema, args: set(args) == {"text"} and type(args["text"]) is str,
        clock=lambda: 1)


def test_discovery_separate_from_invocation_and_validation():
    transport = Transport()
    value = client(transport)
    receipt = value.discover(deadline=10)
    assert transport.calls == ["initialize", "notifications/initialized", "tools/list"]
    assert receipt["tools"] == ["echo"] and len(receipt["capability_digest"]) == 64
    assert value.invoke(value.session, "bad", "echo", {}, deadline=10)["status"] == "rejected"
    result = value.invoke(value.session, "call", "echo", {"text": "hi"}, deadline=10)
    assert result["status"] == "observed"
    with pytest.raises(ContractError):
        value.invoke(value.session, "call", "echo", {"text": "hi"}, deadline=10)
    assert transport.calls.count("tools/call") == 1
    value.close()
    assert value.state == "closed"


def test_denied_or_cancelled_never_dispatch_and_timeout_is_unknown():
    transport = Transport()
    denied = client(transport, allowed=False)
    denied.discover(deadline=10)
    assert denied.invoke(denied.session, "call", "echo", {"text": "hi"},
                         deadline=10)["status"] == "rejected"
    assert "tools/call" not in transport.calls
    value = client(transport)
    value.discover(deadline=10)
    assert value.invoke(value.session, "cancel", "echo", {"text": "hi"}, deadline=10,
                        cancelled=lambda: True)["status"] == "rejected"
    transport.fail = True
    result = value.invoke(value.session, "call", "echo", {"text": "hi"}, deadline=10)
    assert result["status"] == "unknown" and "PRIVATE" not in str(result)
    assert value.state == "degraded"
    assert transport.calls.count("tools/call") == 1


def test_repeated_cursor_fails_discovery_without_partial_activation():
    transport = Transport()
    original = transport.request

    def repeating(method, params, **kwargs):
        if method == "tools/list":
            return {"tools": [], "nextCursor": "same"}
        return original(method, params, **kwargs)

    transport.request = repeating
    value = client(transport)
    with pytest.raises(ContractError, match="discovery failed"):
        value.discover(deadline=10)
    assert value.state == "degraded" and value.capability_digest is None
