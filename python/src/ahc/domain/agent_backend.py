"""Vendor-neutral, immutable agent-turn bindings and observations."""

from dataclasses import dataclass, field
from hashlib import sha256
from math import isfinite

from .result_artifacts import ResultArtifact, result_artifacts
from .runtime_policy import EffectivePolicy
from .sessions import ContractError, VirtualSessionIdentity, _identifier
from .token_observation import TokenObservation
from .turns import _input_text, _sha256_hex


@dataclass(frozen=True, slots=True)
class AgentTurnRequest:
    session: VirtualSessionIdentity
    operation_id: str
    goal_digest: str
    input_text: str
    revision_digests: tuple[str, ...]
    artifact_digest: str
    configuration_digest: str
    workspace_scope: str
    deadline: float
    backend_thread: str | None = None
    purpose: str = "agent.turn"
    capabilities: tuple[str, ...] = ()
    upstream_request_limit: int | None = None
    skill_bodies: tuple[tuple[str, str], ...] = ()
    effective_policy: EffectivePolicy | None = None

    time_budget: object | None = field(default=None, compare=False, repr=False)

    @property
    def effective_deadline(self):
        return self.time_budget.current_deadline() if self.time_budget is not None else self.deadline

    @property
    def deadline_source(self):
        return self.time_budget.current_deadline if self.time_budget is not None else self.deadline

    def __post_init__(self) -> None:
        if self.effective_policy is not None and type(self.effective_policy) is not EffectivePolicy:
            raise ContractError("backend effective policy invalid")
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("backend session type invalid")
        for value in (self.operation_id, self.workspace_scope):
            _identifier(value, "backend binding")
        if self.backend_thread is not None:
            _identifier(self.backend_thread, "backend thread")
        for value in (self.goal_digest, self.artifact_digest, self.configuration_digest):
            _sha256_hex(value)
        _input_text(self.input_text)
        if type(self.revision_digests) is not tuple or len(self.revision_digests) > 64:
            raise ContractError("revision availability must be a bounded immutable tuple")
        for value in self.revision_digests:
            _sha256_hex(value)
        if len(set(self.revision_digests)) != len(self.revision_digests):
            raise ContractError("duplicate revision binding")
        if type(self.skill_bodies) is not tuple or len(self.skill_bodies) > 64:
            raise ContractError("skill bodies must be a bounded immutable tuple")
        total = 0
        for entry in self.skill_bodies:
            if type(entry) is not tuple or len(entry) != 2 or type(entry[1]) is not str:
                raise ContractError("invalid skill body binding")
            _sha256_hex(entry[0])
            if not entry[1].strip() or any(0xD800 <= ord(c) <= 0xDFFF for c in entry[1]):
                raise ContractError("invalid skill body text")
            total += len(entry[1].encode("utf-8"))
        if total > 98_304:
            raise ContractError("skill bodies exceed disclosure bound")
        if self.skill_bodies and tuple(e[0] for e in self.skill_bodies) != self.revision_digests:
            raise ContractError("skill bodies differ from selected revisions")
        if type(self.deadline) not in (int, float) or not isfinite(self.deadline):
            raise ContractError("deadline must be finite")
        if self.purpose != "agent.turn":
            raise ContractError("unsupported backend purpose")
        if type(self.capabilities) is not tuple or len(self.capabilities) > 32:
            raise ContractError("capabilities must be a bounded immutable tuple")
        for value in self.capabilities:
            _identifier(value, "capability")
        if self.upstream_request_limit is not None and (
            type(self.upstream_request_limit) is not int or self.upstream_request_limit < 0
        ):
            raise ContractError("request limit must be a nonnegative integer")

    @property
    def input_digest(self) -> str:
        return sha256(self.input_text.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class AgentTurnObservation:
    """Backend completion is evidence; AHC still owns task/result acceptance."""

    request: AgentTurnRequest
    disposition: str
    reason: str
    admissions: int
    accepted_backend_turns: int
    backend_thread: str | None = None
    backend_turn: str | None = None
    output_text: str = ""
    unresolved_effects: bool = False
    # None means unavailable, never an inferred count of upstream requests.
    vendor_usage: None = None
    artifacts: tuple[ResultArtifact, ...] = ()
    token_observation: TokenObservation | None = None
    cleanup_pending: bool = False
    backend_diagnostic: dict | None = None
    reply_to_message_id: str | None = None
    confirmed_work: dict | None = None
    # Verified model terminal report; it does not accept the original task/effects.
    final_report: bool = False
    backend_recovery: dict | None = None

    def public_dict(self) -> dict[str, object]:
        """Explicit projection: no arbitrary vendor payload/exception representation."""
        from .backend_diagnostics import diagnostic_projection
        from .confirmed_work import confirmed_work_projection
        from .channel_reply import reply_message_id
        from .backend_recovery import recovery_projection

        if self.reply_to_message_id is not None:
            reply_message_id(self.reply_to_message_id)
        tokens = self.token_observation
        if tokens is not None and (type(tokens) is not TokenObservation
                or tokens.backend_thread != self.backend_thread
                or tokens.backend_turn != self.backend_turn):
            raise ContractError("backend token observation binding invalid")
        return {
            "operation_id": self.request.operation_id,
            "session": self.request.session.to_dict(),
            "disposition": self.disposition,
            "reason": self.reason,
            "admissions": self.admissions,
            "accepted_backend_turns": self.accepted_backend_turns,
            "vendor_usage": None,
            **({"token_observation": tokens.to_dict()} if tokens is not None else {}),
            "backend_thread": self.backend_thread,
            "backend_turn": self.backend_turn,
            "output_text": self.output_text,
            "unresolved_effects": self.unresolved_effects,
            "cleanup_pending": self.cleanup_pending,
            **recovery_projection(self.backend_recovery),
            **({"final_report": True} if self.final_report else {}),
            **diagnostic_projection(self.backend_diagnostic),
            **confirmed_work_projection(self.confirmed_work, self.request),
            **({"artifacts": result_artifacts(self.artifacts)} if self.artifacts else {}),
            **({"reply_to_message_id": self.reply_to_message_id}
               if self.reply_to_message_id is not None else {}),
        }
