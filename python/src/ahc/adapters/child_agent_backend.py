"""Single-use, tool-free child turn over an explicitly supplied backend factory."""

import time

from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.child_result import ChildResult
from ahc.domain.child_work import ChildWorkEnvelope, child_output
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError
from ahc.domain.turns import _sha256_hex


class ChildAgentBackend:
    """Factory owns partial construction; a returned backend is closed by this port."""

    def __init__(self, envelope, request, factory, *, parent_request,
                 configuration_digest=None, monotonic=time.monotonic,
                 wall_ms=lambda: time.time_ns() // 1_000_000, artifact_tools=False,
                 observe=None):
        if configuration_digest is not None:
            _sha256_hex(configuration_digest)
        if (type(envelope) is not ChildWorkEnvelope or type(request) is not AgentTurnRequest
                or type(artifact_tools) is not bool
                or envelope.execution_kind != "subagent"
                or type(parent_request) is not AgentTurnRequest
                or request_fingerprint(parent_request) != envelope.parent.request_digest
                or parent_request.session != envelope.parent.session
                or parent_request.operation_id != envelope.parent.operation_id
                or request.session != envelope.parent.session
                or request.operation_id != envelope.child_id
                or request.operation_id == envelope.parent.operation_id
                or request.input_text != envelope.task_text
                or request.workspace_scope != envelope.artifact_scope
                or request.workspace_scope != parent_request.workspace_scope
                or request.deadline > parent_request.effective_deadline
                or request.effective_policy != parent_request.effective_policy
                or request.configuration_digest != (
                    parent_request.configuration_digest if configuration_digest is None
                    else configuration_digest)
                or request.artifact_digest != parent_request.artifact_digest
                or not set(request.revision_digests) <= set(parent_request.revision_digests)
                or not set(request.skill_bodies) <= set(parent_request.skill_bodies)
                or request.backend_thread is not None
                or request.upstream_request_limit is not None
                or parent_request.upstream_request_limit is not None
                or request.capabilities != (("workspace.publish_text", "workspace.attach")
                                            if artifact_tools else ())
                or (artifact_tools and "workspace.artifact" not in parent_request.capabilities)
                or (observe is not None and not callable(observe))
                or not callable(factory) or not callable(monotonic) or not callable(wall_ms)):
            raise ContractError("child backend binding invalid")
        self.envelope, self.request, self.factory = envelope, request, factory
        self.monotonic, self.wall_ms = monotonic, wall_ms
        self.backend = None
        self.artifact_tools = artifact_tools
        self.observe = observe
        self.used = self.closed = False

    def run(self, envelope, *, cancelled):
        if self.used or self.closed or envelope != self.envelope or not callable(cancelled):
            raise ContractError("child backend port cannot be reused")
        self.used = True
        now = self.monotonic()
        remaining = (envelope.deadline_unix_ms - self.wall_ms()) / 1000
        if (cancelled() or remaining <= 0 or self.request.effective_deadline <= now
                or self.request.effective_deadline > now + remaining):
            raise ContractError("child backend deadline or cancellation")
        self.backend = self.factory(self.request, cancelled=cancelled)
        if (not callable(getattr(self.backend, "invoke", None))
                or not callable(getattr(self.backend, "close", None))):
            raise ContractError("child backend lifecycle invalid")
        if cancelled():
            raise ContractError("child backend cancelled before invocation")
        observed = self.backend.invoke(self.request)
        if (self.observe is not None and type(observed) is AgentTurnObservation
                and observed.request == self.request):
            self.observe(observed)
        if (cancelled() or self.monotonic() >= self.request.effective_deadline
                or self.wall_ms() >= envelope.deadline_unix_ms
                or type(observed) is not AgentTurnObservation
                or observed.request != self.request or observed.disposition != "completed"
                or type(observed.admissions) is not int or observed.admissions != 1
                or type(observed.accepted_backend_turns) is not int
                or observed.accepted_backend_turns != 1 or observed.unresolved_effects is not False
                or (observed.artifacts and not self.artifact_tools)):
            raise ContractError("child backend completion unverified")
        if self.artifact_tools:
            return ChildResult(observed.output_text, observed.artifacts)
        return child_output(observed.output_text)

    def close(self):
        if self.closed:
            return
        if self.backend is not None:
            self.backend.close()
        self.closed = True
