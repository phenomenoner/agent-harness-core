import http.client
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from ahc.adapters.mcp_http import McpHttpTransport
from ahc.adapters.mcp_stdio import McpTransportError


@pytest.mark.parametrize("mode", ["resume", "wrong-id", "second-eof", "no-cursor", "long-retry",
                                  "reset", "reset-no-cursor", "reset-cancelled"])
def test_disconnected_sse_resumes_once_without_reposting_tool(mode, monkeypatch):
    observed = []
    cancelled = threading.Event()
    if mode in ("reset", "reset-no-cursor", "reset-cancelled"):
        original = http.client.HTTPResponse.read1
        reads = [0]

        def interrupted_read(response, size):
            reads[0] += 1
            if reads[0] == 2:
                if mode == "reset-cancelled":
                    cancelled.set()
                raise ConnectionResetError("controlled stream reset")
            return original(response, size)
        monkeypatch.setattr(http.client.HTTPResponse, "read1", interrupted_read)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, body):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            observed.append(("POST", value["id"], value["method"]))
            body = b"id: cursor-1\nretry: 0\ndata:\n\n"
            if mode in ("no-cursor", "reset-no-cursor"):
                body = b": heartbeat\n\n"
            if mode == "long-retry":
                body = b"id: cursor-1\nretry: 100000\ndata:\n\n"
            self.reply(body)

        def do_GET(self):
            observed.append(("GET", self.headers.get("Last-Event-ID"),
                             self.headers.get("Mcp-Session-Id")))
            assert self.headers.get("Accept") == "text/event-stream"
            assert self.headers.get("MCP-Protocol-Version") == "2025-11-25"
            body = json.dumps({"jsonrpc": "2.0", "id": 2 if mode == "wrong-id" else 1,
                               "result": {"content": []}}).encode()
            self.reply(b"id: cursor-2\ndata:\n\n" if mode == "second-eof"
                       else b"data: " + body + b"\n\n")

        def do_DELETE(self):
            self.send_response(204)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
    thread.start()
    transport = McpHttpTransport(f"http://127.0.0.1:{server.server_port}/mcp")
    transport.session_id, transport.protocol = "session", "2025-11-25"
    try:
        if mode in ("resume", "reset"):
            assert transport.request("tools/call", {}, deadline=time.monotonic() + 2) == {
                "content": []}
        else:
            with pytest.raises(McpTransportError):
                transport.request("tools/call", {}, deadline=time.monotonic() + 2,
                                  cancelled=cancelled.is_set)
        expected = [("POST", 1, "tools/call")]
        if mode not in ("no-cursor", "long-retry", "reset-no-cursor", "reset-cancelled"):
            expected.append(("GET", "cursor-1", "session"))
        assert observed == expected
    finally:
        transport.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert not thread.is_alive()
