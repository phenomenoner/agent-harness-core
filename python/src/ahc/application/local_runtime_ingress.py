"""Bind one local submission to foreground-owned execution configuration."""

import time
from math import isfinite

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.turns import _sha256_hex
from ahc.infrastructure.working_repository import WorkingWriterBusy


class LocalRuntimeIngress:
    def __init__(
        self,
        inbox,
        *,
        artifact_digest,
        configuration_digest,
        workspace_scope,
        turn_timeout=30,
        clock=time.monotonic,
        effective_policy=None,
    ):
        if effective_policy is not None and type(effective_policy) is not EffectivePolicy:
            raise ContractError("local ingress effective policy invalid")
        self.inbox = inbox
        self.artifact_digest = _sha256_hex(artifact_digest)
        self.workspace_scope = _identifier(workspace_scope, "workspace scope")
        if (
            type(turn_timeout) not in (int, float)
            or not isfinite(turn_timeout)
            or turn_timeout <= 0
            or (effective_policy is None and turn_timeout > 300)
        ):
            raise ContractError("invalid local turn timeout or legacy 300-second limit")
        if not callable(configuration_digest):
            raise ContractError("foreground configuration binding must be callable")
        self.configuration_digest = configuration_digest
        self.turn_timeout = turn_timeout
        self.clock = clock
        self.effective_policy = effective_policy

    def __call__(self, expected, tools):
        try:
            pending = self.inbox.next_pending(expected.session)
        except WorkingWriterBusy:
            return None  # Read-only selection; the durable submission is still pending.
        if pending is None:
            return None
        if (
            pending["session"] != expected.session.to_dict()
            or pending["goal_digest"] != expected.goal.digest
        ):
            raise ContractError("local submission differs from foreground snapshot")
        if tools.workspace is not None and (
            tools.workspace.store is not self.inbox.store
            or tools.workspace.identity.session != expected.session
            or tools.workspace.identity.workspace_id != self.workspace_scope
        ):
            raise ContractError("local ingress workspace binding mismatch")
        # Configuration identity is computed locally; input never supplies executable,
        # capabilities, provider, filesystem paths, auth or an upstream request budget.
        return AgentTurnRequest(
            expected.session,
            pending["operation_id"],
            pending["goal_digest"],
            pending["input_text"],
            (),
            self.artifact_digest,
            self.configuration_digest(tools),
            self.workspace_scope,
            self.clock() + self.turn_timeout,
            capabilities=tuple(tools.capabilities()),
            effective_policy=self.effective_policy,
        )
