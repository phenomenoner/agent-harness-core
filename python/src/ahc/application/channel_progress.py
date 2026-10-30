"""Bounded synchronous progress inside one live admitted turn."""

import time
from uuid import uuid4

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.channel_presentation import typed_payload
from ahc.domain.sessions import ContractError, _identifier
from ahc.infrastructure.channel_outbox import _receipt
from ahc.infrastructure.channel_presentation import operation_kind
from ahc.infrastructure.channel_progress_store import ChannelProgressStore
from ahc.infrastructure.runtime_channel_store import _route
from ahc.infrastructure.runtime_execution_scope import workspace_request


class ChannelProgress:
    def __init__(self, store, request, route, transport, *, cancelled=lambda: False,
                 clock=time.monotonic, wall_clock=time.time):
        if (type(request) is not AgentTurnRequest or type(route) is not ChannelRoute
                or route.session != request.session
                or not callable(getattr(transport, "send", None))
                or not callable(cancelled) or not callable(clock) or not callable(wall_clock)):
            raise ContractError("progress binding invalid")
        self.store, self.request, self.route, self.transport = store, request, route, transport
        self.cancelled, self.clock = cancelled, clock
        self.wall_clock, self.ledger = wall_clock, ChannelProgressStore(store)
        self._calls, self._attempts, self._next_at = set(), 0, float("-inf")

    def send(self, call_id, text):
        _identifier(call_id, "progress call")
        if (type(text) is not str or not 1 <= len(text) <= 512
                or any(0xD800 <= ord(c) <= 0xDFFF for c in text)):
            raise ContractError("progress text invalid")
        request = self.request
        if workspace_request(self.store, request.session, request.workspace_scope) != request:
            raise ContractError("progress belongs to another turn")
        if call_id in self._calls or len(self._calls) >= 16:
            raise ContractError("progress call repeated or exhausted")
        now = self.clock()
        if self.cancelled() or now >= request.effective_deadline:
            return {"status": "stopped"}
        with self.store._read() as connection:
            current = self.store._load_current(connection, request.session.ref.virtual_session_id)
            row = connection.execute(
                "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (request.operation_id,)
            ).fetchone()
            if (row is None or _route(row) != self.route or current.session != request.session
                    or current.status != "active"
                    or current.active_turn_id != request.operation_id):
                raise ContractError("progress turn is no longer active on this route")
            if operation_kind(connection, request.operation_id) == "scheduled":
                return {"status": "suppressed"}
        self._calls.add(call_id)
        if self._attempts >= 4 or now < self._next_at:
            return {"status": "limited"}
        token = uuid4().hex
        if not self.ledger.reserve(request, self.route, call_id, text, token,
                                   int(self.wall_clock() * 1000)):
            return {"status": "limited"}
        self._attempts += 1
        self._next_at = now + 5
        try:
            receipt = self.transport.send(
                self.route, typed_payload({"output_text": text}, "progress"),
                request.operation_id + ":" + call_id)
            receipt = _receipt(receipt)
        except Exception:
            self.ledger.finish(request, self.route, call_id, token, None)
            return {"status": "unknown"}
        self.ledger.finish(request, self.route, call_id, token, receipt)
        return {"status": "sent", "receipt": receipt}
