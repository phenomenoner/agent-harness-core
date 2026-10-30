import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from ahc.adapters.mcp_http import McpHttpTransport
from ahc.adapters.mcp_stdio import McpTransportError


@pytest.mark.parametrize("status,outcome", [(200, "terminated"), (204, "terminated"),
    (404, "expired"), (405, "unsupported"), (307, "unknown"), (500, "unknown")])
def test_session_close_is_exact_bounded_and_not_replayed(status, outcome):
    observed = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_DELETE(self):
            observed.append((self.path, self.headers.get("Mcp-Session-Id"),
                self.headers.get("MCP-Protocol-Version"), self.headers.get("Authorization")))
            self.send_response(status)
            self.send_header("Location", "/must-not-follow")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
    thread.start()
    transport = McpHttpTransport(f"http://127.0.0.1:{server.server_port}/mcp?selected=1",
                                 authorization="fixture-token")
    transport.session_id = "fixture-session"
    transport.protocol = "2025-11-25"
    try:
        for _ in range(2):
            if outcome == "unknown":
                with pytest.raises(McpTransportError):
                    transport.close()
            else:
                transport.close()
        assert transport.close_status == outcome
        assert observed == [("/mcp?selected=1", "fixture-session", "2025-11-25", "fixture-token")]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert not thread.is_alive()


def test_uninitialized_close_has_no_network_effect(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("uninitialized transport must not connect")

    monkeypatch.setattr("http.client.HTTPConnection.connect", forbidden)
    transport = McpHttpTransport("http://127.0.0.1:1/mcp")
    transport.close()
    transport.close()
    assert transport.close_status == "local-only"
