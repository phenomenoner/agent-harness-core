"""Personal WSL Codex composition with explicit home, policy and owner decisions."""

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.trusted_codex_launch import trusted_codex_command
from ahc.adapters.wsl_stream_runner import WslStreamRunner
from ahc.adapters.wsl_unit_runner import WslUnitRunner
from ahc.application.custodied_backend import CustodiedBackend
from ahc.application.pending_approval import PendingApprovalPort
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore

from .foreground_media import compose_foreground_media
from .runtime_environment import ENVIRONMENT_INSTRUCTIONS, environment_context


class TrustedCodexBackendFactory:
    def __init__(self, store, config, *, approval_port_factory=None, tool_gate=None, tool_free=False):
        if type(tool_free) is not bool:
            raise ContractError("tool-free backend flag invalid")
        self.tool_free = tool_free
        self.store, self.config = store, config
        if approval_port_factory is not None and not callable(approval_port_factory):
            raise ContractError("approval port factory invalid")
        self.approval_port_factory = approval_port_factory
        self.tool_gate = tool_gate
        workspace, backend = config.workspace, config.backend
        from ahc.adapters.codex_context_policy import catalog_window

        self._context_window = (catalog_window(backend["codex_home"], backend["model"])
                                if backend.get("context_policy") is not None else None)
        self.native = backend["kind"] == "codex-linux-trusted"
        self.host_executable = workspace.get("wsl_executable")
        self.distribution = workspace.get("distribution")
        from .foreground_user_access import USER_FULL_ACCESS, require_user_full_access

        document = config.to_dict()
        self.user_full_access = self.native and document.get("execution_profile") == USER_FULL_ACCESS
        launch_options = {}
        if self.user_full_access:
            from .foreground_user_environment import native_workspace_environment

            require_user_full_access(document)
            launch_options["user_environment"] = native_workspace_environment(document)
            if "protection" in backend:
                # Preserve the existing history database location while removing
                # its namespace wrapper, without migrating or copying state.
                launch_options["sqlite_home"] = backend["codex_home"] + "/state"
        protected = self.native and not self.user_full_access and "protection" in backend
        self.staged_authentication = protected and backend.get("authentication") == "staged-codex-file"
        from ahc.infrastructure.process.linux_backend_boundary import BACKEND_HOME

        self._command = trusted_codex_command(
            bundle=backend["bundle"],
            codex_home=BACKEND_HOME if protected else backend["codex_home"],
            cwd=workspace["cwd"],
            interpreter=workspace["interpreter"],
            approval_policy=config.effective_policy.codex_approval_policy,
            sealed_history=protected,
            staged_authentication=self.staged_authentication,
            tool_free=tool_free,
            **launch_options,
        )

    def _client(self, transport, tools, cancelled, approval_port=None, answer_seal=None, *, child=False):
        if isinstance(approval_port, PendingApprovalPort):
            approval_port.activity_event = getattr(tools, "activity_event", None)
        client = CodexProtocolClient(
            transport,
            cwd=self.config.workspace["cwd"],
            model=self.config.backend["model"],
            reasoning_effort=self.config.backend.get("reasoning_effort"),
            service_tier=self.config.backend.get("service_tier"),
            cancelled=cancelled,
            workspace_tools=tools,
            effective_policy=self.config.effective_policy,
            approval_port=approval_port,
            launch_configuration_digest=self.config.configuration_digest,
            target="x86_64-unknown-linux-musl",
            developer_instructions=(
                self.config.to_dict().get("agent_context", {}).get("instructions")),
            environment_instructions=ENVIRONMENT_INSTRUCTIONS,
            answer_seal=answer_seal,
            turn_media=compose_foreground_media(self.store, tools),
            close_transport=False,  # CustodiedBackend owns transport cleanup and retries.
            isolated_provider_auth=self.staged_authentication,
            startup_timeout=self.config.backend.get('startup_timeout'),
            request_timeout=self.config.backend.get('request_timeout'),
            settlement_timeout=self.config.backend.get('settlement_timeout'),
            wrap_up_timeout=self.config.backend.get('wrap_up_timeout'),
            settlement_within_task_deadline=child,
            context_policy=self.config.backend.get("context_policy"),
            context_window=self._context_window,
            overload_recovery=(None if child else self.config.backend.get("overload_recovery")),
        )
        # Children also use this client factory, including tool-free children.
        # Parent composition replaces this with bounded conversation/memory context.
        client.turn_context = lambda request: environment_context(self.config, tools)
        return client

    def binding(self, tools, *, child=False):
        client = (self._client(None, tools, lambda: False, child=True) if child
                  else self._client(None, tools, lambda: False))
        return client.executable_sha256, client.configuration_digest

    def new_runner(self, *, recovery=False):
        if self.native:
            from ahc.adapters.native_process_runner import NativeProcessRunner

            from .foreground_backend_protection import native_backend_protection

            options = {} if self.user_full_access else (
                native_backend_protection(self.config, provider_auth=True)
                if self.staged_authentication
                else native_backend_protection(self.config)
            )
            adaptive = self.config.backend.get("time_budget", {}).get("enabled", False)
            return NativeProcessRunner(cwd=self.config.workspace["cwd"], environment={},
                                       persistent=adaptive, owner_lifetime=adaptive, **options)
        if recovery:
            return WslUnitRunner(self.host_executable, self.distribution)
        return WslStreamRunner(self.host_executable, self.distribution, long_deadlines=True)

    def recover(self, session, workspace_id):
        from ahc.infrastructure.child_backend_recovery import recover_child_backends

        runner = self.new_runner(recovery=True)
        recover_child_backends(self.store, session, workspace_id, runner)
        custody = BackendCustody(self.store)
        lease = custody.acquire(session, workspace_id)
        try:
            return custody.recover(lease, runner)
        finally:
            lease.close()

    def __call__(self, request, tools, *, cancelled=lambda: False):
        if tools.workspace.store is not self.store:
            raise ContractError("trusted Codex workspace store mismatch")
        artifact, configuration = self.binding(tools)
        if (
            request.artifact_digest != artifact
            or request.configuration_digest != configuration
            or request.effective_policy != self.config.effective_policy
        ):
            raise ContractError("trusted Codex request differs from launch binding")
        skills = getattr(tools, "skills", None)
        if skills is not None and callable(getattr(skills, "bind_request", None)):
            skills.bind_request(request, tools.workspace)
        runner = self.new_runner()
        owner = self

        def attach(unit):
            transport = runner.attach(unit, backend.custody, backend.lease)

            class Client:
                def invoke(self, bound):
                    port = (
                        PendingApprovalPort(RuntimeApprovalStore(owner.store), bound)
                        if owner.approval_port_factory is None
                        else owner.approval_port_factory(bound)
                    )
                    from ahc.domain.runtime_turn import accept_observation

                    client = owner._client(
                        transport,
                        tools,
                        cancelled,
                        port,
                        answer_seal=lambda observed: owner.store.seal_observation(
                            bound, accept_observation(bound, observed)
                        ),
                    )
                    client.activity_event = getattr(tools, "activity_event", None)
                    if bound.time_budget is not None:
                        bound.time_budget.client = client
                        bound.time_budget.cancelled = cancelled
                    controls = getattr(owner.store, "channel_controls", None)
                    if controls is not None:
                        client.steering_port = controls.steering_port(bound)
                    if owner.tool_gate is not None:
                        client.tool_gate = owner.tool_gate(bound, port, cancelled=cancelled)
                    from .foreground_turn_context import turn_context

                    client.turn_context = lambda request: turn_context(
                        owner.store, owner.config, tools, request)
                    return client.run_admitted(bound)

                def close(self):
                    transport.close()

            return Client()

        backend = CustodiedBackend(
            self.store,
            tools.workspace.identity,
            runner,
            self._command,
            self.config.backend["lifetime_seconds"],
            attach,
            custody=BackendCustody(self.store),
        )
        return backend
