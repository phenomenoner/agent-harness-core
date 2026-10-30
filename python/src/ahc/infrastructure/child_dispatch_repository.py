"""Bind local child dispatch to an admitted parent request and its lifetime lease."""

import math
import time
from contextlib import contextmanager

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.child_work import ChildParent, ChildWorkEnvelope
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError, _identifier

from .child_work_repository import ChildWorkRepository
from .runtime_execution_scope import permits_child_parent
from .working_repository import WorkingSetConflict


class ChildDispatchRepository(ChildWorkRepository):
    def __init__(
        self,
        store,
        request,
        *,
        clock=lambda: time.time_ns() // 1_000_000,
        monotonic_clock=time.monotonic,
    ):
        if type(request) is not AgentTurnRequest or not callable(monotonic_clock):
            raise ContractError("child dispatch requires a parent request and clock")
        super().__init__(store, clock=clock)
        self.request, self.monotonic_clock = request, monotonic_clock
        self.parent = ChildParent(
            request.session, request.operation_id, request_fingerprint(request)
        )

    def _bound(self, parent):
        if parent != self.parent:
            raise ContractError("child dispatch parent mismatch")

    def _monotonic_now(self):
        now = self.monotonic_clock()
        if type(now) not in (int, float) or not math.isfinite(now):
            raise ContractError("child dispatch monotonic clock invalid")
        return now

    @contextmanager
    def execution_scope(self, parent):
        self._bound(parent)
        if permits_child_parent(self.store, parent):
            yield
        else:
            with self.store.execution_lock():
                yield

    def admit(self, envelope):
        if type(envelope) is not ChildWorkEnvelope:
            raise ContractError("child dispatch envelope invalid")
        self._bound(envelope.parent)
        if envelope.artifact_scope != self.request.workspace_scope:
            raise ContractError("child artifact scope exceeds parent binding")
        existing = any(item["child_id"] == envelope.child_id for item in self.read(self.parent))
        if not existing:
            now = self._monotonic_now()
            remaining = max(0, self.request.effective_deadline - now)
            if envelope.deadline_unix_ms > self._now() + remaining * 1000:
                raise ContractError("child deadline exceeds parent deadline")
        return super().admit(envelope)

    def should_stop(self, parent, child_id, token):
        self._bound(parent)
        _identifier(child_id, "child identifier")
        _identifier(token, "child claim token")
        with self.store._read() as connection:
            try:
                self._require_parent(connection, parent)
            except WorkingSetConflict:
                return True
            row, envelope, _ = self._row(connection, parent, child_id)
            return (
                row["state"] != "claimed"
                or row["claim_token"] != token
                or envelope.deadline_unix_ms <= self._now()
                or self._monotonic_now() >= self.request.effective_deadline
            )

    @contextmanager
    def backend_admission(self, envelope, token):
        """Hold parent execution ownership and verify the exact durable child claim."""
        if type(envelope) is not ChildWorkEnvelope:
            raise ContractError("child backend envelope invalid")
        self._bound(envelope.parent)
        _identifier(token, "child claim token")
        with self.execution_scope(envelope.parent):
            with self.store._read() as connection:
                self._require_parent(connection, envelope.parent)
                row, recorded, _ = self._row(connection, envelope.parent, envelope.child_id)
                if (recorded != envelope or row["state"] != "claimed"
                        or row["claim_token"] != token
                        or envelope.deadline_unix_ms <= self._now()
                        or self._monotonic_now() >= self.request.effective_deadline
                        or envelope.artifact_scope != self.request.workspace_scope):
                    raise ContractError("child backend requires a current durable claim")
            yield

    def mark_unknown(self, parent, child_id, token, *, observations=()):
        self._bound(parent)
        _identifier(child_id, "child identifier")
        _identifier(token, "child claim token")

        def write(connection):
            self._require_parent(connection, parent)
            row, envelope, result = self._row(connection, parent, child_id)
            if row["state"] == "claimed":
                if row["claim_token"] != token:
                    raise ContractError("child claim belongs to another token")
                connection.execute(
                    "UPDATE runtime_children SET state='unknown',claim_token=NULL WHERE child_id=?",
                    (child_id,),
                )
                from .child_observations import record
                record(connection, envelope, observations)
                row, envelope, result = self._row(connection, parent, child_id)
            return self._record(row, envelope, result, connection)

        return self.store._mutate(write)

    def reconcile_completion(self, envelope, token, output):
        """Read a lost local completion reply once; never execute or complete again."""
        from ahc.domain.child_result import normalize_child_result

        if type(envelope) is not ChildWorkEnvelope:
            raise ContractError("child completion envelope invalid")
        self._bound(envelope.parent)
        _identifier(token, "child claim token")
        payload = normalize_child_result(output).to_dict()

        def readback(connection):
            self._require_parent(connection, envelope.parent)
            row, recorded, result = self._row(connection, envelope.parent, envelope.child_id)
            if recorded != envelope:
                raise ContractError("child completion envelope mismatch")
            if row["state"] in ("claimed", "terminal") and row["claim_token"] != token:
                raise ContractError("child completion claim mismatch")
            if row["state"] == "terminal" and result != payload:
                raise ContractError("child completion result mismatch")
            from .child_observations import record
            record(connection, envelope, ("completion-reply-lost",))
            if row["state"] == "claimed":
                record(connection, envelope, ("completion-not-committed",))
                connection.execute(
                    "UPDATE runtime_children SET state='unknown',claim_token=NULL "
                    "WHERE child_id=?", (envelope.child_id,)
                )
                row, recorded, result = self._row(connection, envelope.parent, envelope.child_id)
            return self._record(row, recorded, result, connection)

        return self.store._mutate(readback)
