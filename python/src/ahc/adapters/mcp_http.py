"""Explicit MCP POST transport; no redirects, ambient auth or automatic replay."""

import http.client
import json
import math
import socket
import threading
import time
from urllib.parse import urlsplit

from ahc.domain.sessions import ContractError

from .codex_transport import _object
from .mcp_sse_resume import ResumeStream, resume_metadata
from .mcp_stdio import McpTransportError


class McpHttpTransport:
    def __init__(self, endpoint, *, authorization=None):
        url = urlsplit(endpoint)
        if (url.scheme not in ("http", "https") or not url.hostname or url.username
                or url.password or url.fragment
                or (url.scheme == "http" and url.hostname not in ("127.0.0.1", "::1", "localhost"))
                or (authorization is not None and (type(authorization) is not str
                    or "\r" in authorization or "\n" in authorization))):
            raise ContractError("MCP HTTP endpoint or authorization invalid")
        self.url, self.authorization = url, authorization
        self.session_id = None
        self.protocol = None
        self._sequence = 0
        self._thread = threading.get_ident()
        self._closed = False
        self.close_status = "open"

    def request(self, method, params, *, deadline, cancelled=lambda: False):
        self._sequence += 1
        return self._post({"jsonrpc": "2.0", "id": self._sequence,
                           "method": method, "params": params}, deadline, cancelled)

    def notify(self, method, params, *, deadline):
        self._post({"jsonrpc": "2.0", "method": method, "params": params},
                   deadline, lambda: False)

    def _post(self, message, deadline, cancelled, *, verb="POST", cursor=None):
        if ((self._closed and verb != "DELETE") or threading.get_ident() != self._thread
                or type(deadline) not in (int, float) or not math.isfinite(deadline)
                or not 0 < deadline - time.monotonic() <= 300 or cancelled()):
            raise McpTransportError("MCP HTTP request unavailable")
        body = json.dumps(message, allow_nan=False).encode()
        if len(body) > 65536:
            raise ContractError("MCP HTTP payload too large")
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json, text/event-stream"}
        if cursor is not None:
            headers["Last-Event-ID"] = cursor
            headers["Accept"] = "text/event-stream"
        if self.authorization is not None:
            headers["Authorization"] = self.authorization
        if self.session_id is not None:
            headers["Mcp-Session-Id"] = self.session_id
        if self.protocol is not None:
            headers["MCP-Protocol-Version"] = self.protocol
        connection_type = (http.client.HTTPSConnection if self.url.scheme == "https"
                           else http.client.HTTPConnection)
        connection = connection_type(self.url.hostname, self.url.port,
                                     timeout=deadline - time.monotonic())
        stopped, interrupted = threading.Event(), threading.Event()
        sockets = []

        def guard():
            while not stopped.wait(0.025):
                if cancelled() or time.monotonic() >= deadline:
                    interrupted.set()
                    for stream in sockets:
                        try:
                            stream.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                    return

        watcher = threading.Thread(target=guard, daemon=True)
        watcher.start()
        try:
            connection.connect()
            sockets.append(connection.sock)
            if interrupted.is_set() or cancelled() or time.monotonic() >= deadline:
                raise TimeoutError()
            target = self.url.path or "/"
            if self.url.query:
                target += "?" + self.url.query
            connection.request(verb, target, body=body if verb == "POST" else None, headers=headers)
            response = connection.getresponse()
            if verb == "DELETE":
                outcomes = {200: "terminated", 204: "terminated", 404: "expired",
                            405: "unsupported"}
                if response.status not in outcomes:
                    raise ValueError()
                return outcomes[response.status]
            if "id" not in message:
                if response.status != 202:
                    raise ValueError()
                return None
            if response.status != 200:
                raise ValueError()
            content_type = response.getheader("Content-Type", "").split(";")[0].strip()
            if content_type not in ("application/json", "text/event-stream"):
                raise ValueError()
            if verb == "GET" and content_type != "text/event-stream":
                raise ValueError()
            data = bytearray()
            while True:
                try:
                    chunk = response.read1(4096)
                except (OSError, http.client.IncompleteRead):
                    if (content_type == "text/event-stream" and verb == "POST"
                            and message["method"] != "initialize" and not cancelled()
                            and not interrupted.is_set() and time.monotonic() < deadline):
                        next_cursor, delay = resume_metadata(bytes(data))
                        if next_cursor is not None:
                            raise ResumeStream(next_cursor, delay) from None
                    raise
                if interrupted.is_set() or cancelled() or time.monotonic() >= deadline:
                    raise TimeoutError()
                data.extend(chunk)
                if len(data) > 65536:
                    raise ValueError()
                if content_type == "text/event-stream":
                    result = self._sse(bytes(data), message["id"])
                    if result is not None:
                        break
                if not chunk:
                    if content_type != "application/json":
                        next_cursor, delay = resume_metadata(bytes(data))
                        if (verb == "POST" and message["method"] != "initialize"
                                and next_cursor is not None):
                            raise ResumeStream(next_cursor, delay)
                        raise ValueError()
                    result = self._result(bytes(data), message["id"])
                    break
            if message["method"] == "initialize":
                token = response.getheader("Mcp-Session-Id")
                if token is not None and (not token or len(token) > 1024
                        or any(not 33 <= ord(char) <= 126 for char in token)):
                    raise ValueError()
                self.session_id = token
                self.protocol = result.get("protocolVersion")
            return result
        except ResumeStream as resume:
            next_cursor, delay = resume.cursor, resume.delay_ms
        except Exception:
            self._closed = True
            raise McpTransportError("MCP HTTP response unverified") from None
        finally:
            stopped.set()
            watcher.join(timeout=1)
            connection.close()
        # The original socket and watchdog are closed before the sole recovery GET.
        retry_at = time.monotonic() + delay / 1000
        while time.monotonic() < retry_at:
            if cancelled() or retry_at >= deadline:
                self._closed = True
                raise McpTransportError("MCP SSE resume exceeds request lifetime")
            time.sleep(min(0.025, max(0, retry_at - time.monotonic())))
        return self._post(message, deadline, cancelled, verb="GET", cursor=next_cursor)

    @staticmethod
    def _result(raw, identity):
        value = json.loads(raw, object_pairs_hook=_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if (type(value) is not dict or value.get("jsonrpc") != "2.0"
                or type(value.get("id")) is not int or value["id"] != identity
                or "error" in value or "result" not in value):
            raise ValueError()
        return value["result"]

    @classmethod
    def _sse(cls, raw, identity):
        blocks = raw.replace(b"\r\n", b"\n").split(b"\n\n")
        for block in blocks[:-1]:
            data = b"\n".join(line[5:].lstrip(b" ") for line in block.split(b"\n")
                               if line.startswith(b"data:"))
            if data:
                message = json.loads(data, object_pairs_hook=_object)
                if (type(message) is dict and message.get("jsonrpc") == "2.0"
                        and "method" in message and "id" not in message):
                    if message["method"] == "notifications/tools/list_changed":
                        raise ValueError()
                    continue
                return cls._result(data, identity)
        return None

    def close(self):
        if threading.get_ident() != self._thread:
            raise McpTransportError("MCP HTTP close requires owner thread")
        if self.close_status != "open":
            if self.close_status == "unknown":
                raise McpTransportError("MCP HTTP termination unverified; no automatic retry")
            return
        self._closed = True
        if self.session_id is None:
            self.close_status = "local-only"
            return
        self.close_status = "unknown"
        self.close_status = self._post({}, time.monotonic() + 5, lambda: False, verb="DELETE")
