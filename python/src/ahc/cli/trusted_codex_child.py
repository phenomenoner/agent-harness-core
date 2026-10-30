"""Explicit tool-free Codex child composition; constructing a factory starts no process."""

from dataclasses import replace

from ahc.adapters.child_agent_backend import ChildAgentBackend
from ahc.application.custodied_child_backend import CustodiedChildBackend
from ahc.application.pending_approval import PendingApprovalPort
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_approval_store import ChildApprovalStore
from ahc.infrastructure.child_backend_custody import ChildBackendCustody


class TrustedCodexChildFactory:
    def __init__(self, parent_factory, children, envelope, token, workspace):
        if (parent_factory.store is not children.store
                or envelope.parent != children.parent
                or workspace.session != envelope.parent.session
                or workspace.workspace_id != envelope.artifact_scope
                or not parent_factory.config.backend["enabled"]):
            raise ContractError("trusted child composition mismatch or disabled")
        self.parent_factory, self.children = parent_factory, children
        self.envelope, self.token, self.workspace = envelope, token, workspace
        from .foreground_child_limits import owner_limits
        self.tools = None
        if (owner_limits(parent_factory).get("max_artifacts", 0)
                and "workspace.artifact" in children.request.capabilities):
            from ahc.infrastructure.child_artifact_tools import ChildArtifactTools
            self.tools = ChildArtifactTools(children, envelope, token, workspace)

    def binding(self):
        return self.parent_factory.binding(self.tools, child=True)

    def build_request(self):
        parent = self.children.request
        artifact, configuration = self.binding()
        # Sample monotonic first and reserve the wall clock's millisecond quantum.
        now = self.children._monotonic_now()
        remaining = (self.envelope.deadline_unix_ms - self.children._now() - 1) / 1000
        deadline = min(parent.effective_deadline, now + remaining)
        if remaining <= 0 or deadline <= now:
            raise ContractError("trusted child request deadline expired")
        return replace(parent, operation_id=self.envelope.child_id,
            input_text=self.envelope.task_text, artifact_digest=artifact,
            configuration_digest=configuration, deadline=deadline, time_budget=None,
            backend_thread=None, capabilities=(() if self.tools is None
                                               else self.tools.capabilities()))

    def port(self, request=None):
        if request is None:
            request = self.build_request()
        artifact, configuration = self.binding()
        if (request.artifact_digest != artifact
                or request.effective_policy != self.parent_factory.config.effective_policy):
            raise ContractError("trusted child launch binding mismatch")
        from ahc.infrastructure.child_token_observations import record

        return ChildAgentBackend(self.envelope, request, self._backend,
            parent_request=self.children.request, configuration_digest=configuration,
            monotonic=self.children._monotonic_now, wall_ms=self.children._now,
            artifact_tools=self.tools is not None,
            observe=lambda observed: record(self.children, self.envelope, self.token, observed))

    def _backend(self, request, *, cancelled):
        owner = self.parent_factory
        runner = owner.new_runner()
        custody = ChildBackendCustody(self.children.store, self.envelope,
            worker_permit=getattr(self.children, "permit", None))

        def attach(unit):
            transport = runner.attach(unit, backend.custody, backend.lease)

            class Client:
                def invoke(client_self, admitted):
                    approvals = ChildApprovalStore(
                        self.children, self.envelope, self.token, admitted)
                    controls = getattr(owner.store, "channel_controls", None)
                    notifier = None
                    if controls is not None:
                        def notifier(pending):
                            controls.notify_approval(pending,
                                parent_operation=self.children.request.operation_id,
                                child_store=approvals)
                    port = PendingApprovalPort(approvals, admitted, notifier=notifier)
                    client = owner._client(transport, self.tools, cancelled, port, child=True)
                    if owner.tool_gate is not None:
                        client.tool_gate = owner.tool_gate(admitted, port, cancelled=cancelled)
                    return client.run_admitted(admitted)

                def close(client_self):
                    transport.close()

            return Client()

        backend = CustodiedChildBackend(self.children, self.envelope, self.token, request,
            self.workspace, runner, owner._command, owner.config.backend["lifetime_seconds"],
            attach, custody=custody)
        return backend
