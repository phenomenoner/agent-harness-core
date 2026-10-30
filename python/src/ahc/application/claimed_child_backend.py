"""Construct a child backend only under its actual child-ledger admission."""

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError, _identifier


class ClaimedChildBackend:
    """The supplied factory must own a child-specific process, never the parent lease."""

    def __init__(self, children, envelope, token, request, factory):
        if (type(envelope) is not ChildWorkEnvelope or type(request) is not AgentTurnRequest
                or request.session != envelope.parent.session
                or request.operation_id != envelope.child_id
                or request.input_text != envelope.task_text
                or request.workspace_scope != envelope.artifact_scope or not callable(factory)):
            raise ContractError("claimed child backend binding invalid")
        _identifier(token, "child claim token")
        self.children, self.envelope, self.token = children, envelope, token
        self.request, self.factory = request, factory
        self.backend = None
        self.used = self.closed = False

    def invoke(self, request):
        if self.used or self.closed or request != self.request:
            raise ContractError("claimed child backend cannot be replayed")
        self.used = True
        with self.children.backend_admission(self.envelope, self.token):
            self.backend = self.factory()
            if (not callable(getattr(self.backend, "invoke", None))
                    or not callable(getattr(self.backend, "close", None))):
                raise ContractError("claimed child backend lifecycle invalid")
            if self.children.should_stop(self.envelope.parent, self.envelope.child_id, self.token):
                raise ContractError("child stopped during backend construction")
            return self.backend.invoke(request)

    def close(self):
        if self.closed:
            return
        if self.backend is not None:
            self.backend.close()
        self.closed = True
