"""Independent child ownership over the existing exact-unit custody mechanism."""

from contextlib import contextmanager

from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError

from .backend_custody import BackendCustody


class ChildBackendCustody(BackendCustody):
    """Custody is not admission; construction still requires the claimed-child guard."""

    def __init__(self, repository, envelope, *, worker_permit=None):
        if type(envelope) is not ChildWorkEnvelope or envelope.execution_kind != "subagent":
            raise ContractError("child custody envelope invalid")
        self._envelope = envelope
        if worker_permit is not None:
            from .runtime_execution_scope import ChildWorkerPermit
            if type(worker_permit) is not ChildWorkerPermit:
                raise ContractError("child custody requires an exact worker permit")
            worker_permit.require(repository, envelope)
        self._worker_permit = worker_permit
        super().__init__(repository)

    @contextmanager
    def _execution_lock(self, lease):
        if self._worker_permit is not None:
            try:
                self._worker_permit.require(self.repository, self._envelope)
            except ContractError:
                pass  # Parent recovery must acquire its ordinary execution scope.
            else:
                yield
                return
        with super()._execution_lock(lease):
            yield

    @property
    def envelope(self):
        return self._envelope

    def _role_binding(self):
        return {"resource_role": "backend", "child": {
            "parent": self._envelope.parent.to_dict(),
            "child_id": self._envelope.child_id,
            "envelope_digest": self._envelope.request_digest}}

    def acquire(self, session, workspace):
        if (session != self._envelope.parent.session
                or workspace != self._envelope.artifact_scope):
            raise ContractError("child custody owner mismatch")
        return super().acquire(session, workspace)
