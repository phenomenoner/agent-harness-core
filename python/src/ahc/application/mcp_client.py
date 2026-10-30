"""MCP tools lifecycle over an explicit deadline-aware transport port."""

import json
import math
import threading
import time

from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.skill_values import canonical_json, digest_json

PROTOCOL = "2025-11-25"


class McpClient:
    """Transport owns framing/correlation, cancellation, close and I/O deadlines.

    authorize(session, server_id, tool, capability_digest) is AHC-owned, not
    inferred from server annotations. validate(schema, arguments) must implement
    the admitted JSON Schema dialect; absence never silently permits a call.
    """

    def __init__(self, session, server_id, transport, *, authorize, validate,
                 clock=time.monotonic, receipts=None):
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("MCP session invalid")
        self.server_id = _identifier(server_id, "MCP server")
        self.session, self.transport = session, transport
        self.authorize, self.validate, self.clock = authorize, validate, clock
        self.state = "new"
        self._thread = threading.get_ident()
        self._tools = {}
        self._used = set()
        self.capability_digest = None
        self.receipts = receipts

    def _owner(self):
        if self._thread != threading.get_ident():
            raise ContractError("MCP client requires owner thread")

    def _budget(self, deadline, cancelled):
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ContractError("MCP deadline invalid")
        return not cancelled() and self.clock() < deadline

    @staticmethod
    def _json(value, *, max_bytes=65536):
        encoded = canonical_json(value)
        if len(encoded) > max_bytes:
            raise ContractError("MCP payload limit exceeded")
        return json.loads(encoded)

    def discover(self, *, deadline, cancelled=lambda: False):
        self._owner()
        if self.state != "new" or not self._budget(deadline, cancelled):
            raise ContractError("MCP discovery unavailable")
        self.state = "initializing"
        try:
            initialized = self.transport.request("initialize", {
                "protocolVersion": PROTOCOL, "capabilities": {},
                "clientInfo": {"name": "ahc", "version": "0.3.0"},
            }, deadline=deadline, cancelled=cancelled)
            initialized = self._json(initialized)
            if (initialized.get("protocolVersion") != PROTOCOL
                    or type(initialized.get("capabilities")) is not dict
                    or type(initialized["capabilities"].get("tools")) is not dict):
                raise ContractError("MCP tool negotiation unsupported")
            self.transport.notify("notifications/initialized", {}, deadline=deadline)
            tools, cursors, cursor = {}, set(), None
            for _ in range(16):
                if not self._budget(deadline, cancelled):
                    raise ContractError("MCP discovery interrupted")
                page = self._json(self.transport.request("tools/list",
                    {} if cursor is None else {"cursor": cursor},
                    deadline=deadline, cancelled=cancelled), max_bytes=524288)
                if type(page.get("tools")) is not list:
                    raise ContractError("MCP discovery invalid")
                for tool in page["tools"]:
                    if (type(tool) is not dict or type(tool.get("name")) is not str
                            or not tool["name"] or len(tool["name"]) > 128
                            or tool["name"] in tools or len(tools) >= 256
                            or type(tool.get("inputSchema")) is not dict):
                        raise ContractError("MCP tool schema invalid")
                    tools[tool["name"]] = tool
                cursor = page.get("nextCursor")
                if cursor is None:
                    break
                if type(cursor) is not str or not cursor or cursor in cursors:
                    raise ContractError("MCP discovery cursor invalid")
                cursors.add(cursor)
            else:
                raise ContractError("MCP discovery page limit exceeded")
            self.capability_digest = digest_json({"server_id": self.server_id,
                "protocol": PROTOCOL, "tools": [tools[key] for key in sorted(tools)]})
            self._tools, self.state = tools, "ready"
            return {"capability_digest": self.capability_digest, "tools": sorted(tools)}
        except Exception:
            self.state = "degraded"
            raise ContractError("MCP discovery failed") from None

    def invoke(self, session, call_id, tool, arguments, *, deadline, cancelled=lambda: False):
        self._owner()
        _identifier(call_id, "MCP call")
        if session != self.session or call_id in self._used or len(self._used) >= 256:
            raise ContractError("MCP call owner or identity invalid")
        if self.state != "ready" or type(tool) is not str or tool not in self._tools:
            return {"call_id": call_id, "status": "rejected", "reason": "unavailable"}
        try:
            args = self._json(arguments)
            schema = self._json(self._tools[tool]["inputSchema"])
            admitted = (type(args) is dict and self._budget(deadline, cancelled)
                and self.authorize(session, self.server_id, tool, self.capability_digest) is True
                and self.validate(schema, args) is True)
        except Exception:
            admitted = False
        if not admitted:
            return {"call_id": call_id, "status": "rejected", "reason": "admission"}
        if self.receipts is not None:
            try:
                self.receipts.begin(session, call_id, self.server_id,
                                    self.capability_digest, tool, args)
            except Exception:
                return {"call_id": call_id, "status": "rejected", "reason": "durable-admission"}
        self._used.add(call_id)
        try:
            result = self._json(self.transport.request("tools/call", {
                "name": tool, "arguments": args}, deadline=deadline, cancelled=cancelled))
            if (type(result) is not dict or type(result.get("content")) is not list
                    or type(result.get("isError", False)) is not bool):
                raise ContractError("MCP result invalid")
            status = "tool-error" if result.get("isError") else "observed"
            if self.receipts is not None:
                self.receipts.finish(call_id, status)
            return {"call_id": call_id, "status": status, "result": result}
        except Exception:
            self.state = "degraded"
            if self.receipts is not None:
                try:
                    self.receipts.finish(call_id, "unknown")
                except Exception:
                    pass  # Pending reservation remains completion-unverified; never replay.
            return {"call_id": call_id, "status": "unknown", "reason": "response-unverified"}

    def close(self):
        self._owner()
        self.transport.close()
        self.state = "closed"
