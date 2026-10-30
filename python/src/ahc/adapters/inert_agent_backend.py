"""Bounded bidirectional protocol experiment. No executable, socket or auth path.

The fixture dialect deliberately does not claim compatibility with any installed
Codex version. Production launch/enforcement and durable admission remain open.
"""

import json
import time
from collections.abc import Callable

from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.sessions import ContractError, _identifier


class ProtocolFault(Exception):
    pass


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolFault("duplicate field")
        result[key] = value
    return result


class InertPeer:
    """Finite serialized inbound frames and serialized outbound transcript only."""

    def __init__(self, frames: tuple[bytes, ...]) -> None:
        if type(frames) is not tuple or len(frames) > 128:
            raise ContractError("inert transcript exceeds frame budget")
        if any(type(frame) is not bytes or len(frame) > 16384 for frame in frames):
            raise ContractError("inert frame exceeds byte budget")
        self.frames = iter(frames)
        self.sent: list[bytes] = []
        self.closed = False

    def send(self, message: dict[str, object]) -> None:
        self.sent.append(json.dumps(message, allow_nan=False).encode("utf-8"))

    def receive(self) -> dict[str, object]:
        try:
            message = json.loads(next(self.frames), object_pairs_hook=_object)
        except (StopIteration, ValueError, RecursionError) as exc:
            raise ProtocolFault("missing or invalid frame") from exc
        if type(message) is not dict:
            raise ProtocolFault("frame must be object")
        return message


class InertAgentBackend:
    """One-use driver with fail-closed admission and conservative unknown results."""

    def __init__(
        self, peer: InertPeer, *, clock: Callable[[], float] = time.monotonic,
        cancelled: Callable[[], bool] = lambda: False,
    ) -> None:
        if type(peer) is not InertPeer:
            raise ContractError("only the in-memory inert peer is admitted")
        self.peer = peer
        self.clock = clock
        self.cancelled = cancelled
        self.used = False

    def invoke(self, request: AgentTurnRequest) -> AgentTurnObservation:
        if type(request) is not AgentTurnRequest:
            raise ContractError("invalid agent turn request")
        if self.used:
            raise ContractError("backend instance consumed; replay prohibited")
        self.used = True
        admitted = accepted = 0
        submitted = False
        thread = turn = None

        def result(status: str, reason: str, output: str = "") -> AgentTurnObservation:
            return AgentTurnObservation(
                request, status, reason, admitted, accepted, thread, turn, output,
                unresolved_effects=submitted and status != "completed",
            )

        try:
            # This fixture has no effects. No caller-provided envelope upgrades it.
            if request.capabilities or request.upstream_request_limit is not None:
                return result("unsupported", "envelope-not-enforceable")
            if self.clock() >= request.effective_deadline or self.cancelled():
                return result("not-submitted", "deadline-or-cancel")
            admitted = 1
            self.peer.send({"id": 1, "method": "initialize", "params": {"protocol": "inert-v1"}})
            self._response(1, {"protocol": "inert-v1"})
            self.peer.send({"method": "initialized", "params": {}})
            method = "thread/resume" if request.backend_thread else "thread/start"
            self.peer.send({
                "id": 2, "method": method, "params": {"threadId": request.backend_thread},
            })
            response = self._response(2)
            if set(response) != {"threadId"}:
                raise ProtocolFault("thread shape")
            thread = _identifier(response["threadId"], "thread")
            if request.backend_thread is not None and thread != request.backend_thread:
                raise ProtocolFault("thread binding")
            if self.clock() >= request.effective_deadline or self.cancelled():
                return result("not-submitted", "deadline-or-cancel")
            # Mark possible submission before writing, including ambiguous write failure.
            submitted = True
            self.peer.send({"id": 3, "method": "turn/start", "params": {
                "threadId": thread, "operationId": request.operation_id,
                "session": request.session.to_dict(), "goalDigest": request.goal_digest,
                "input": request.input_text, "inputDigest": request.input_digest,
                "revisionDigests": list(request.revision_digests),
                "artifactDigest": request.artifact_digest,
                "configurationDigest": request.configuration_digest,
                "workspaceScope": request.workspace_scope, "capabilities": [],
            }})
            response = self._response(3)
            if set(response) != {"threadId", "turnId"} or response["threadId"] != thread:
                raise ProtocolFault("turn binding")
            turn = _identifier(response["turnId"], "turn")
            accepted = 1
            for _ in range(128):
                if self.clock() >= request.effective_deadline or self.cancelled():
                    self.peer.send({"id": 4, "method": "turn/interrupt", "params": {
                        "threadId": thread, "turnId": turn,
                    }})
                    return result("unknown", "cancel-unreconciled")
                message = self.peer.receive()
                if "id" in message and "method" in message:
                    # Never inspect or expose untrusted arguments; no request is approved.
                    request_id = message["id"]
                    if type(request_id) is not int or not 0 <= request_id <= 2**31 - 1:
                        raise ProtocolFault("server request id")
                    self.peer.send({"id": request_id, "error": {
                        "code": -32601, "message": "unsupported method",
                    }})
                    return result("unknown", "server-request-rejected")
                if set(message) != {"method", "params"}:
                    raise ProtocolFault("notification shape")
                params = message["params"]
                if type(params) is not dict or params.get("threadId") != thread:
                    raise ProtocolFault("notification thread")
                if params.get("turnId") != turn:
                    raise ProtocolFault("notification turn")
                if message["method"] == "turn/progress":
                    if set(params) != {"threadId", "turnId"}:
                        raise ProtocolFault("progress shape")
                    continue
                if message["method"] != "turn/completed":
                    raise ProtocolFault("unsupported notification")
                if set(params) != {"threadId", "turnId", "status", "output"}:
                    raise ProtocolFault("terminal shape")
                if params["status"] not in ("completed", "failed", "interrupted"):
                    raise ProtocolFault("terminal status")
                output = params["output"]
                if type(output) is not str or len(output) > 4096:
                    raise ProtocolFault("output bound")
                if any(0xD800 <= ord(char) <= 0xDFFF for char in output):
                    raise ProtocolFault("output unicode")
                if params["status"] != "completed":
                    return result("unknown", "backend-non-success")
                return result("completed", "backend-observed", output)
            raise ProtocolFault("event budget")
        except (ProtocolFault, ContractError, OSError):
            return result("unknown" if submitted else "not-submitted", "protocol-or-transport")
        finally:
            self.peer.closed = True

    def _response(self, request_id: int, expected: dict[str, object] | None = None) -> dict:
        message = self.peer.receive()
        if set(message) != {"id", "result"} or type(message["id"]) is not int:
            raise ProtocolFault("response shape")
        if message["id"] != request_id or type(message["result"]) is not dict:
            raise ProtocolFault("response correlation")
        if expected is not None and message["result"] != expected:
            raise ProtocolFault("response contract")
        return message["result"]
