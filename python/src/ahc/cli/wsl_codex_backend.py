"""Explicit foreground composition; no CLI activation or provider discovery.

The owner supplies the admitted Linux policy/artifacts and model configuration.
Constructing a factory or request binding does not start a process or model turn.
"""

import json

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.linux_codex_launch import LaunchRejected, command_for, validate_spec
from ahc.adapters.wsl_codex_stream_runner import WslCodexStreamRunner
from ahc.adapters.wsl_unit_runner import WslUnitRunner
from ahc.application.custodied_backend import CustodiedBackend
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.backend_custody import BackendCustody


class WslCodexBackendFactory:
    def __init__(
        self, store, host_executable, distribution, *, launch_spec, model, lifetime_seconds=180
    ):
        # Validate/freeze the same specification shape as the protected runner.
        try:
            validate_spec(launch_spec)
        except LaunchRejected as exc:
            raise ContractError(str(exc)) from None
        probe = WslCodexStreamRunner(host_executable, distribution, launch_spec=launch_spec)
        self._spec_json = probe._launch_json
        self.store = store
        self.host_executable, self.distribution = probe.host_executable, probe.distribution
        self.model = model
        if type(lifetime_seconds) is not int or not 1 <= lifetime_seconds <= 3600:
            raise ContractError("invalid backend unit lifetime")
        self.lifetime_seconds = lifetime_seconds
        self._command = tuple(command_for(json.loads(self._spec_json)))
        self._launch_digest = digest_json(
            {
                "specification": json.loads(self._spec_json),
                "distribution": self.distribution,
                "host_executable": self.host_executable,
                "lifetime_seconds": lifetime_seconds,
            }
        )

    def _client(self, transport, tools, cancelled, answer_seal=None):
        spec = json.loads(self._spec_json)
        return CodexProtocolClient(
            transport,
            cwd=spec["workspace"],
            model=self.model,
            cancelled=cancelled,
            workspace_tools=tools,
            permissions=spec["profile"],
            policy_configuration_digest=self._launch_digest,
            target="x86_64-unknown-linux-musl",
            answer_seal=answer_seal,
            close_transport=False,  # CustodiedBackend owns transport cleanup and retries.
        )

    def binding(self, tools):
        client = self._client(None, tools, lambda: False)
        return client.executable_sha256, client.configuration_digest

    def recover(self, session, workspace_id):
        """Reconcile held units before opening a replacement workspace, even when idle."""
        custody = BackendCustody(self.store)
        lease = custody.acquire(session, workspace_id)
        try:
            return custody.recover(lease, WslUnitRunner(self.host_executable, self.distribution))
        finally:
            lease.close()

    def __call__(self, request, tools, *, cancelled=lambda: False):
        if tools.workspace.store is not self.store:
            raise ContractError("Codex workspace belongs to another runtime store")
        artifact, configuration = self.binding(tools)
        if request.artifact_digest != artifact or request.configuration_digest != configuration:
            raise ContractError("Codex request differs from protected launch binding")
        runner = WslCodexStreamRunner(
            self.host_executable, self.distribution, launch_spec=json.loads(self._spec_json)
        )

        def client_factory(unit):
            transport = runner.attach(unit, backend.custody, backend.lease)
            owner = self

            class Client:
                def invoke(self, bound):
                    # CustodiedBackend owns this object before client construction.
                    # Failed construction/close therefore retains transport custody.
                    from ahc.domain.runtime_turn import accept_observation

                    client = owner._client(
                        transport,
                        tools,
                        cancelled,
                        answer_seal=lambda observed: owner.store.seal_observation(
                            bound, accept_observation(bound, observed)
                        ),
                    )
                    return client.run_admitted(bound)

                def close(self):
                    transport.close()

            return Client()

        backend = CustodiedBackend(
            self.store,
            tools.workspace.identity,
            runner,
            self._command,
            self.lifetime_seconds,
            client_factory,
            custody=BackendCustody(self.store),
        )
        return backend
