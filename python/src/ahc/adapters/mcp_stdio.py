"""Single-owner MCP JSON-RPC over existing Windows-owned bounded JSONL pipes."""

import json
import queue
import threading
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.infrastructure.process.managed import spawn_managed

from .codex_transport import CodexStdioTransport, _object


class McpTransportError(RuntimeError):
    pass


class _Jsonl(CodexStdioTransport):
    MAX_FRAME = 524288  # Bounded multi-tool discovery pages; call payload limits stay separate.
    MAX_OUTBOUND_FRAME = 65536

    def receive_cancelable(self, deadline, cancelled):
        while True:
            if cancelled():
                raise McpTransportError("MCP request cancelled after dispatch")
            remaining = self._remaining(deadline)
            if self._closed or self._fault.is_set() or (
                    self._eof.is_set() and self._frames.empty()):
                raise McpTransportError("MCP stream unavailable")
            try:
                raw = self._frames.get(timeout=min(remaining, 0.05))
            except queue.Empty:
                continue
            if raw is None:
                raise McpTransportError("MCP stream ended")
            value = json.loads(raw, object_pairs_hook=_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
            if type(value) is not dict:
                raise McpTransportError("MCP frame invalid")
            return value


class McpStdioTransport:
    @classmethod
    def launch(cls, command, *, cwd, env, protection=None, lifetime_seconds=None):
        if (type(command) is not tuple or not command or len(command) > 64
                or any(type(arg) is not str or "\0" in arg for arg in command)
                or sum(len(arg) for arg in command) > 16384
                or not Path(command[0]).is_absolute() or not Path(command[0]).is_file()
                or not Path(cwd).is_absolute() or not Path(cwd).is_dir()
                or type(env) is not dict):
            raise ContractError("MCP launch configuration invalid")
        runner = owned = None
        try:
            if protection is None:
                if lifetime_seconds is not None:
                    raise ContractError("MCP native lifetime requires protection")
                owned = spawn_managed(command, cwd=str(cwd), env=dict(env), protocol_pipes=True)
            else:
                from .native_process_runner import NativeProcessRunner

                if (type(protection) is not dict
                        or set(protection) != {"boundary", "resource_limits"}
                        or protection["boundary"] is None
                        or protection["resource_limits"] is None
                        or env):
                    raise ContractError(
                        "protected MCP requires an exact native profile and empty environment")
                runner = NativeProcessRunner(cwd=str(cwd), environment={}, persistent=True,
                                             **protection)
                attempt = runner.prepare_creation(command, lifetime_seconds=lifetime_seconds)
                runner.create_attempt(attempt, command, lifetime_seconds=lifetime_seconds)
                owned = runner._owned
            # Foreground admission permits turns up to one hour. A longer
            # remaining budget is valid, not an already-expired deadline.
            return cls(_Jsonl(owned, streaming=True, max_wait=3600), runner=runner)
        except BaseException:
            if runner is not None:
                runner.close()
            elif owned is not None:
                owned.cleanup(5)
            raise

    def __init__(self, wire, *, runner=None):
        self.wire = wire
        self._runner = runner
        self._thread = threading.get_ident()
        self._sequence = 0
        self._closed = False

    def _owner(self):
        if self._thread != threading.get_ident() or self._closed:
            raise McpTransportError("MCP transport unavailable")

    def notify(self, method, params, *, deadline):
        self._owner()
        try:
            self.wire.send({"jsonrpc": "2.0", "method": method, "params": params},
                           deadline=deadline)
        except Exception:
            self.close()
            raise McpTransportError("MCP notification unverified") from None

    def request(self, method, params, *, deadline, cancelled=lambda: False):
        self._owner()
        if cancelled():
            raise McpTransportError("MCP request cancelled before dispatch")
        self._sequence += 1
        identity = self._sequence
        try:
            self.wire.send({"jsonrpc": "2.0", "id": identity, "method": method,
                            "params": params}, deadline=deadline)
            for _ in range(128):
                frame = self.wire.receive_cancelable(deadline, cancelled)
                if frame.get("jsonrpc") != "2.0":
                    raise ValueError()
                if "method" in frame:
                    if "id" not in frame:
                        # Tool-list invalidation requires a fresh discovery, never stale grants.
                        if frame["method"] == "notifications/tools/list_changed":
                            raise ValueError()
                        continue
                    if frame["method"] == "ping":
                        reply = {"result": {}}
                    else:
                        reply = {"error": {"code": -32601, "message": "Unsupported method"}}
                    self.wire.send({"jsonrpc": "2.0", "id": frame["id"], **reply},
                                   deadline=deadline)
                    continue
                if type(frame.get("id")) is not int or frame["id"] != identity:
                    raise ValueError()
                if ("error" in frame) == ("result" in frame):
                    raise ValueError()
                if "error" in frame:
                    raise McpTransportError("MCP remote request failed")
                return frame["result"]
            raise McpTransportError("MCP response frame budget exceeded")
        except Exception:
            self.close()
            raise McpTransportError("MCP response unverified") from None

    def close(self):
        if self._closed:
            return
        if self._thread != threading.get_ident():
            raise McpTransportError("MCP cleanup requires owner thread")
        try:
            try:
                self.wire.close()
            finally:
                if self._runner is not None:
                    self._runner.close()
        except Exception:
            raise McpTransportError("MCP cleanup unverified") from None
        self._closed = True
