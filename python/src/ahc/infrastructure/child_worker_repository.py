"""Exact-child ledger view for a worker whose parent retains the execution lease."""

from contextlib import contextmanager

from ahc.domain.sessions import ContractError

from .child_dispatch_repository import ChildDispatchRepository
from .runtime_execution_scope import ChildWorkerPermit


class ChildWorkerRepository(ChildDispatchRepository):
    def __init__(self, children, envelope):
        children._bound(envelope.parent)
        self.envelope = envelope
        self.permit = ChildWorkerPermit(children.store, envelope)
        super().__init__(children.store, children.request, clock=children.clock,
                         monotonic_clock=children.monotonic_clock)

    def _bound(self, parent):
        self.permit.require(self.store, self.envelope)
        super()._bound(parent)

    def _require_parent(self, connection, parent):
        self._bound(parent)
        return super()._require_parent(connection, parent)

    def _row(self, connection, parent, child_id):
        self._bound(parent)
        if child_id != self.envelope.child_id:
            raise ContractError("child worker cannot access a sibling")
        value = super()._row(connection, parent, child_id)
        if value[1] != self.envelope:
            raise ContractError("child worker envelope differs from its durable record")
        return value

    def _records(self, connection, parent):
        self._bound(parent)
        row = connection.execute("SELECT * FROM runtime_children WHERE child_id=?",
                                 (self.envelope.child_id,)).fetchone()
        if row is None:
            return ()
        row, envelope, result = self._row(connection, parent, self.envelope.child_id)
        return (self._record(row, envelope, result, connection),)

    @contextmanager
    def execution_scope(self, parent):
        self._bound(parent)
        yield

    def admit(self, envelope):
        self.permit.require(self.store, envelope)
        return super().admit(envelope)

    @contextmanager
    def backend_admission(self, envelope, token):
        self.permit.require(self.store, envelope)
        with super().backend_admission(envelope, token):
            yield

    def reconcile(self, parent):
        raise ContractError("parent reconciliation belongs to the parent owner")

    @staticmethod
    def retire_parent(connection, parent_operation_id):
        raise ContractError("parent retirement belongs to the parent owner")
