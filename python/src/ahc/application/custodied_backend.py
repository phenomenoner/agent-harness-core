"""Per-turn backend custody composition; executable policy is caller-owned.

The injected client factory must bind the returned Linux identity to its transport.
This module does not enable a provider or select a workload command from model input.
"""

from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError


class CustodiedBackend:
    def __init__(
        self, store, workspace, runner, command, lifetime_seconds, client_factory, *, custody
    ):
        self.store, self.workspace, self.runner = store, workspace, runner
        self.command, self.lifetime_seconds = command, lifetime_seconds
        self.client_factory = client_factory
        # Composition owns the concrete custody implementation and its store binding.
        self.custody = custody
        self.lease = self.client = None
        self.used = self.closed = False

    def invoke(self, request):
        if self.used or self.closed:
            raise ContractError("backend instance cannot be replayed")
        if (
            request.session != self.workspace.session
            or request.workspace_scope != self.workspace.workspace_id
        ):
            raise ContractError("backend workspace binding mismatch")
        turn = self.store.read_turn(request.session, request.operation_id)
        if (
            turn is None
            or turn.status != "submitted"
            or turn.request_digest != request_fingerprint(request)
        ):
            raise ContractError("backend requires a durable admitted turn")
        self.used = True
        self.lease = self.custody.acquire(request.session, request.workspace_scope)
        self.custody.recover(self.lease, self.runner)
        unit = self.custody.create(
            self.lease,
            self.workspace,
            self.runner,
            self.command,
            lifetime_seconds=self.lifetime_seconds,
        )
        self.client = self.client_factory(unit)
        return self.client.invoke(request)

    def recovery_ready(self):
        probe = getattr(self.runner, "control_ready", None)
        return self.lease is not None and callable(probe) and probe()

    def close(self):
        if self.closed:
            return
        if self.client is not None:
            close = getattr(self.client, "close", None)
            if close is not None:
                close()
            self.client = None
        close_runner = getattr(self.runner, "close", None)
        if close_runner is not None:
            close_runner()
        if self.lease is not None:
            self.custody.recover(self.lease, self.runner)
            self.lease.close()
            self.lease = None
        self.closed = True
