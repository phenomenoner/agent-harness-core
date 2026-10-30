"""Cooperative local child execution over one already-admitted parent turn."""

from typing import Protocol

from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError


class SubagentDispatchPort(Protocol):
    def run(self, envelope: ChildWorkEnvelope, *, cancelled) -> str: ...

    def close(self) -> None: ...


class ChildDispatchService:
    def __init__(self, children, factory):
        if not callable(factory):
            raise ContractError("child dispatch factory invalid")
        self.children, self.factory = children, factory
        self._pending_close = None
        self._observations = set()

    @property
    def cleanup_pending(self):
        return self._pending_close is not None

    def _unknown(self, envelope, token):
        return self.children.mark_unknown(envelope.parent, envelope.child_id, token,
            observations=tuple(sorted(self._observations or {"execution-unverified"})))

    def dispatch(self, envelope, token, *, cancelled=lambda: False):
        if type(envelope) is not ChildWorkEnvelope or not callable(cancelled):
            raise ContractError("child dispatch input invalid")
        if self._pending_close is not None:
            raise ContractError("child dispatch close remains pending")
        self._observations = set()
        with self.children.execution_scope(envelope.parent):
            record = self.children.admit(envelope)
            if record["state"] in ("terminal", "cancelled", "unknown"):
                return record
            if record["state"] == "claimed":
                raise ContractError("child dispatch already claimed; no replay")
            if cancelled():
                return self.children.cancel(envelope.parent, envelope.child_id)
            record = self.children.claim(envelope.parent, envelope.child_id, token)
            if record["state"] != "claimed":
                return record
            try:
                if self._stopped(envelope, token, cancelled):
                    return self._unknown(envelope, token)
            except Exception:
                return self._unknown(envelope, token)
            try:
                port = self.factory(envelope)
            except Exception:
                self._observations.add("factory-unverified")
                return self._unknown(envelope, token)
            failed = not callable(getattr(port, "run", None)) or not callable(
                getattr(port, "close", None)
            )
            output = None
            if failed:
                self._observations.add("port-invalid")
            try:
                if not failed:
                    failed = self._stopped(envelope, token, cancelled)
                if not failed:
                    output = port.run(
                        envelope,
                        cancelled=lambda: self._stopped(envelope, token, cancelled),
                    )
                if not failed:
                    failed = self._stopped(envelope, token, cancelled)
            except Exception:
                self._observations.add("execution-unverified")
                failed = True
            finally:
                if callable(getattr(port, "close", None)):
                    try:
                        port.close()
                    except Exception:
                        self._observations.add("cleanup-unverified")
                        self._pending_close = (envelope.parent, port)
                        failed = True
            if failed:
                return self._unknown(envelope, token)
            try:
                stopped = self._stopped(envelope, token, cancelled)
            except Exception:
                stopped = True
            if stopped:
                return self._unknown(envelope, token)
            try:
                return self.children.complete(envelope.parent, envelope.child_id, token, output)
            except ContractError:
                return self._unknown(envelope, token)
            except Exception:
                return self.children.reconcile_completion(envelope, token, output)

    def close(self, *, execution_owner=None):
        if self._pending_close is None:
            return
        parent, port = self._pending_close
        owner = self.children if execution_owner is None else execution_owner
        if (owner.store is not self.children.store
                or owner.request != self.children.request or owner.parent != parent):
            raise ContractError("child cleanup owner mismatch")
        with owner.execution_scope(parent):
            port.close()
            self._pending_close = None

    def _stopped(self, envelope, token, cancelled):
        try:
            if cancelled():
                self._observations.add("cancellation-observed")
                return True
            if (self.children._now() >= envelope.deadline_unix_ms
                    or self.children._monotonic_now() >= self.children.request.effective_deadline):
                self._observations.add("deadline-observed")
                return True
            if self.children.should_stop(envelope.parent, envelope.child_id, token):
                self._observations.add("parent-or-claim-changed")
                return True
            return False
        except Exception:
            self._observations.add("stop-observation-unavailable")
            raise


__all__ = ["ChildDispatchService", "SubagentDispatchPort"]
