"""Channel-side approval boundary for admitted dynamic workspace tools."""

from __future__ import annotations

import re
import time
from math import isfinite

from ahc.domain.runtime_approval import ApprovalDecision, ApprovalRequest
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.infrastructure.working_repository import WorkingWriterBusy

_READ_ONLY_CAPABILITIES = frozenset(
    {
        "workspace.inspect",
        "workspace.environment",
        "workspace.memory_pack",
        "workspace.memory_search",
        "workspace.skills_list",
        "workspace.skills_read",
        "workspace.skills_resources",
        "workspace.read_artifact",
        "workspace.incoming_artifacts",
        "workspace.channel_messages",
    }
)
_PROMPTING_MODES = frozenset({"manual", "auto-approve"})
_AUTO_APPROVING_MODES = frozenset({"always-approve", "yolo"})
_SECRET = re.compile(
    r"(?ix)("
    r"authorization[\"']?\s*:\s*[\"']?bearer\s+|"
    r"\bbearer\s+|"
    r"(?:api[_ -]?key|token|password)[\"']?\s*[:=]\s*[\"']?"
    r")[^\"'\s,}\]]+"
)
_SUMMARY_LIMIT = 1024


class ChannelToolGate:
    """Authorize one exact admitted dynamic-tool invocation, never execute it."""

    def __init__(
        self,
        admitted,
        approval_port,
        *,
        cancelled=lambda: False,
        clock=time.monotonic,
        wall_clock=time.time,
    ):
        self.admitted = admitted
        self.approval_port = approval_port
        self.cancelled = cancelled
        self.clock = clock
        self.wall_clock = wall_clock

    def authorize(self, capability, arguments, thread, turn, call_id) -> bool:
        """Return whether this one call may proceed under the admitted policy."""

        try:
            _identifier(capability, "tool capability")
            _identifier(thread, "tool thread")
            _identifier(turn, "tool turn")
            _identifier(call_id, "tool call")
            if self._stopped() or not self._admitted(capability):
                return False
            mode = self.admitted.effective_policy.approval_mode
            if mode in _AUTO_APPROVING_MODES:
                return True
            if mode not in _PROMPTING_MODES:
                return False
            if capability in _READ_ONLY_CAPABILITIES:
                return True
            if capability == "workspace.learning":
                # Explicitly configured, reversible maintenance has its own
                # scope/protection/kill/CAS admission. No per-revision approval.
                return True
            request = self._request(capability, arguments, thread, turn, call_id)
            if self._stopped():
                return False
            decision = self.approval_port.decide(request, cancelled=self.cancelled)
            if self._stopped() or type(decision) is not ApprovalDecision:
                return False
            return decision.resolve(
                request, now=self.wall_clock(), cancelled=bool(self.cancelled())
            ) == "approve-once"
        except (AttributeError, ContractError, TypeError, ValueError, UnicodeError,
                WorkingWriterBusy):
            return False

    def _admitted(self, capability) -> bool:
        capabilities = self.admitted.capabilities
        return type(capabilities) is tuple and capability in capabilities

    def _stopped(self) -> bool:
        if not callable(self.cancelled) or bool(self.cancelled()):
            return True
        deadline = self.admitted.deadline
        return (
            type(deadline) not in (int, float)
            or not isfinite(deadline)
            or self.clock() >= deadline
        )

    def _request(self, capability, arguments, thread, turn, call_id) -> ApprovalRequest:
        policy = self.admitted.effective_policy
        if policy is None:
            raise ContractError("channel tool policy missing")
        remaining = min(self.admitted.deadline - self.clock(), policy.approval_wait_seconds)
        if remaining <= 0:
            raise ContractError("channel tool approval expired")
        action_digest = digest_json(
            {"capability": capability, "arguments": arguments, "call_id": call_id}
        )
        request_id = f"tool:{call_id}"
        if len(request_id) > 128:
            call_digest = digest_json({"call_id": call_id})
            request_id = f"tool:{call_digest}"
        return ApprovalRequest(
            session=self.admitted.session,
            operation_id=self.admitted.operation_id,
            policy_digest=policy.digest,
            backend_thread=thread,
            backend_turn=turn,
            vendor_request_id=request_id,
            action_digest=action_digest,
            expires_at=self.wall_clock() + remaining,
            action_summary=_action_summary(capability, arguments),
        )


def _action_summary(capability, arguments) -> str:
    preview = _SECRET.sub(r"\1[REDACTED]", canonical_json(arguments).decode("utf-8"))
    text = f"{capability}: {preview}"
    return text[:_SUMMARY_LIMIT]
