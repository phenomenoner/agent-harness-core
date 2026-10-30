"""Child-ledger admission and independent exact-unit lifecycle composition."""

import math

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError, _identifier

from .custodied_backend import CustodiedBackend


class CustodiedChildBackend(CustodiedBackend):
    def __init__(self, children, envelope, token, request, workspace, runner, command,
                 lifetime_seconds, client_factory, *, custody):
        if (type(envelope) is not ChildWorkEnvelope or type(request) is not AgentTurnRequest
                or request.session != envelope.parent.session
                or request.operation_id != envelope.child_id
                or request.input_text != envelope.task_text
                or request.workspace_scope != envelope.artifact_scope
                or workspace.session != request.session
                or workspace.workspace_id != request.workspace_scope
                or getattr(custody, "envelope", None) != envelope
                or getattr(custody, "repository", None) is not children.store
                or type(lifetime_seconds) is not int or lifetime_seconds <= 0):
            raise ContractError("child process binding invalid")
        _identifier(token, "child claim token")
        super().__init__(children.store, workspace, runner, command, lifetime_seconds,
                         client_factory, custody=custody)
        self.children, self.envelope, self.token, self.request = children, envelope, token, request

    def _require_running(self):
        if self.children.should_stop(self.envelope.parent, self.envelope.child_id, self.token):
            raise ContractError("child process stopped before invocation")

    def invoke(self, request):
        if self.used or self.closed or request != self.request:
            raise ContractError("child process cannot be replayed")
        self.used = True
        with self.children.backend_admission(self.envelope, self.token):
            self.lease = self.custody.acquire(request.session, request.workspace_scope)
            self.custody.recover(self.lease, self.runner)
            self._require_running()
            seconds = math.ceil((self.envelope.deadline_unix_ms - self.children._now()) / 1000)
            if seconds <= 0:
                raise ContractError("child process deadline expired")
            unit = self.custody.create(self.lease, self.workspace, self.runner, self.command,
                lifetime_seconds=min(self.lifetime_seconds, seconds))
            self._require_running()
            self.client = self.client_factory(unit)
            self._require_running()
            return self.client.invoke(request)
