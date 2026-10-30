"""Read batches require a current admitted foreground turn and its deadline."""

import threading

from ahc.application.tool_batch import ReadOnlyBatch

from .batch_receipt_repository import BatchReceiptRepository
from .runtime_execution_scope import workspace_request
from .runtime_query_port import RuntimeQueryPort


class RuntimeReadBatch(ReadOnlyBatch):
    def __init__(self, store, session, workspace_id, view):
        super().__init__(session, view, RuntimeQueryPort(store, session))
        self.store, self.workspace_id = store, workspace_id
        self.receipts = BatchReceiptRepository(store)
        self._receipt_lock = threading.Lock()

    def _admit(self, calls):
        self._request = workspace_request(self.store, self.session, self.workspace_id)
        self.receipts.begin(self._request, calls)

    def _invoke(self, call, deadline, cancelled):
        state, reason, value = super()._invoke(call, deadline, cancelled)
        try:
            with self._receipt_lock:
                self.receipts.finish(self._request, call.call_id, state, reason)
        except Exception:
            return "unknown", "receipt-unverified", None
        return state, reason, value

    def run(self, expected, calls, *, deadline, cancelled=lambda: False, concurrency=1):
        request = workspace_request(self.store, self.session, self.workspace_id)

        def stopped():
            current = self.store.read_current(self.session.ref.virtual_session_id)
            turn = self.store.read_turn(self.session, request.operation_id)
            return (cancelled() or current.session != self.session or current.status != "active"
                    or current.goal.digest != request.goal_digest
                    or turn is None or turn.status != "submitted")

        return super().run(expected, calls, deadline=min(deadline, request.effective_deadline),
                           cancelled=stopped, concurrency=concurrency)
