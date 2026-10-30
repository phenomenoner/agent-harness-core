"""Structured ordinary-child dispatch; parent owns joins and deferred cleanup."""

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from uuid import uuid4

from ahc.application.child_dispatch import ChildDispatchService
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError

from .child_worker_repository import ChildWorkerRepository
from .runtime_execution_scope import permits_child_parent
from .threaded_runtime_repository import ThreadedRuntimeRepository


class ChildBatchRunner:
    def __init__(self, children):
        if not isinstance(children.store, ThreadedRuntimeRepository):
            raise ContractError("child batch requires a shared threaded repository")
        self.children = children
        self._lock, self._running = Lock(), False
        self._services = []

    @property
    def cleanup_pending(self):
        return any(service.cleanup_pending for service in self._services)

    def _validate(self, envelopes, factory, cancelled, concurrency):
        if (type(envelopes) not in (tuple, list) or not envelopes
                or type(concurrency) is not int or concurrency < 1
                or not callable(factory) or not callable(cancelled)
                or not permits_child_parent(self.children.store, self.children.parent)):
            raise ContractError("child batch input or parent scope invalid")
        records = {row["child_id"]: row for row in self.children.read(self.children.parent)}
        ids = set()
        now, wall = self.children._monotonic_now(), self.children._now()
        for envelope in envelopes:
            if (type(envelope) is not ChildWorkEnvelope
                    or envelope.parent != self.children.parent
                    or envelope.execution_kind != "subagent"
                    or envelope.artifact_scope != self.children.request.workspace_scope
                    or envelope.child_id in ids):
                raise ContractError("child batch envelope invalid")
            ids.add(envelope.child_id)
            existing = records.get(envelope.child_id)
            if existing is not None:
                if existing["request_digest"] != envelope.request_digest:
                    raise ContractError("child batch replay differs")
            elif not wall < envelope.deadline_unix_ms <= (
                    wall + max(0, self.children.request.effective_deadline - now) * 1000):
                raise ContractError("child batch deadline outside parent")

    def dispatch(self, envelopes, factory, *, cancelled=lambda: False, concurrency=3):
        """Return records in request order; None means dispatch could not be verified.

        factory receives (worker_repository, envelope, token). It must return a
        cooperative bounded port whose close may be retried by the parent thread.
        """
        with self._lock:
            if self._running or self.cleanup_pending:
                raise ContractError("child batch busy or cleanup pending")
            self._validate(envelopes, factory, cancelled, concurrency)
            envelopes = tuple(envelopes)
            self._running = True
        try:
            # Admit the whole validated batch before any backend factory runs.
            for envelope in envelopes:
                self.children.admit(envelope)
            work = []
            for envelope in envelopes:
                worker = ChildWorkerRepository(self.children, envelope)
                token = uuid4().hex
                service = ChildDispatchService(worker, lambda item, w=worker, t=token:
                                               factory(w, item, t))
                self._services.append(service)
                work.append((worker, service, envelope, token))

            def run(item):
                worker, service, envelope, token = item
                try:
                    with worker.permit.activate():
                        return service.dispatch(envelope, token, cancelled=cancelled)
                except Exception:
                    return None

            # Exiting the executor joins even after an exception. Never release
            # the parent's lease while owned backend work is still running.
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                return tuple(executor.map(run, work))
        finally:
            with self._lock:
                self._running = False

    def close(self):
        with self._lock:
            if self._running:
                raise ContractError("child batch workers are still active")
            pending = []
            for service in self._services:
                try:
                    service.close(execution_owner=self.children)
                except Exception:
                    pending.append(service)
            self._services = pending
            if pending:
                raise ContractError("child batch cleanup remains pending")
