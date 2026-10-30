import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from test_runtime_core import setup

from ahc.adapters.mcp_http import McpHttpTransport
from ahc.application.mcp_client import PROTOCOL, McpClient
from ahc.domain.mcp_http_profiles import McpHttpProfile
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.mcp_http_composition import connect_mcp_http_profile
from ahc.infrastructure.mcp_receipts import McpReceipts
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("mode", ["json", "sse", "timeout", "cancel", "wrong",
                                  "resume", "resume-wrong"])
@pytest.mark.parametrize("profile_mode", [False, True])
def test_loopback_http_protocol_and_interrupt(mode, profile_mode, tmp_path):
    if profile_mode:
        pytest.importorskip("jsonschema")
    observed = []
    release = threading.Event()
    resumed = []
    response_body = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_DELETE(self):
            self.send_response(405)
            self.end_headers()

        def do_GET(self):
            resumed.append(self.headers.get("Last-Event-ID"))
            assert self.headers.get("Mcp-Session-Id") == "fixture-session"
            assert self.headers.get("MCP-Protocol-Version") == PROTOCOL
            body = b"data: " + response_body[0] + b"\n\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            msg = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            observed.append((msg["method"], self.headers.get("Mcp-Session-Id"),
                             self.headers.get("MCP-Protocol-Version")))
            if "id" not in msg:
                self.send_response(202)
                self.end_headers()
                return
            if msg["method"] == "initialize":
                result = {"protocolVersion": PROTOCOL, "capabilities": {"tools": {}}}
            elif msg["method"] == "tools/list":
                result = {"tools": [{"name": "echo", "inputSchema": {"type": "object"}}]}
            else:
                if mode in ("timeout", "cancel"):
                    release.wait(2)
                result = {"content": [{"type": "text", "text": "http-result"}]}
            identity = msg["id"] + (mode in ("wrong", "resume-wrong")
                                    and msg["method"] == "tools/call")
            body = json.dumps({"jsonrpc": "2.0", "id": identity, "result": result}).encode()
            recovering = mode in ("resume", "resume-wrong") and msg["method"] == "tools/call"
            if recovering:
                response_body.append(body)
                body = b"id: call-result\nretry: 0\ndata:\n\n"
            if mode == "sse":
                progress = b'data: {"jsonrpc":"2.0","method":"notifications/progress"}\n\n'
                body = progress + b"data: " + body + b"\n\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream" if mode == "sse" or recovering
                             else "application/json")
            if msg["method"] == "initialize":
                self.send_header("Mcp-Session-Id", "fixture-session")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
    thread.start()
    transport = McpHttpTransport(f"http://127.0.0.1:{server.server_port}/mcp")
    session = VirtualSessionIdentity.initial("s", "a", "r")
    client = McpClient(session, "http", transport, authorize=lambda *a: True,
                       validate=lambda *a: True)
    cancel = threading.Event()
    timer = None
    try:
        if profile_mode:
            store, current, request = setup(tmp_path)
            store.begin(current, request)
            session = current.session
            digest = digest_json({"server_id": "http", "protocol": PROTOCOL,
                "tools": [{"name": "echo", "inputSchema": {"type": "object"}}]})
            profile = McpHttpProfile("http", session.agent_id,
                f"http://127.0.0.1:{server.server_port}/mcp", None, ("echo",), digest, True)
            assert McpHttpProfile.from_dict(profile.to_dict()) == profile
            client, _ = connect_mcp_http_profile(profile, store, request,
                                                deadline=time.monotonic() + 3)
        else:
            client.discover(deadline=time.monotonic() + 3)
        if mode == "cancel":
            timer = threading.Timer(0.1, cancel.set)
            timer.start()
        result = client.invoke(session, "call", "echo", {}, cancelled=cancel.is_set,
            deadline=time.monotonic() + (0.15 if mode == "timeout" else 3))
        assert result["status"] == ("observed" if mode in ("json", "sse", "resume") else "unknown")
        assert observed[1][1:] == ("fixture-session", PROTOCOL)
        assert observed[-1][1:] == ("fixture-session", PROTOCOL)
        if profile_mode:
            receipt = McpReceipts(store, request).read()[0]
            assert receipt["status"] == result["status"]
            reopened = McpReceipts(RuntimeRepository(store.root), request).read()
            assert len(reopened) == 1 and reopened[0] == receipt
        if mode in ("resume", "resume-wrong"):
            assert resumed == ["call-result"]
            assert sum(item[0] == "tools/call" for item in observed) == 1
    finally:
        release.set()
        if timer is not None:
            timer.cancel()
            timer.join()
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert not thread.is_alive()
