"""Profile-bound Pack admission; trusted composition owns the provider callable."""

from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDefinition,
    DecisionKind,
    EffectClass,
)
from ahc.domain.memory_pack import _text
from ahc.domain.memory_pack_response import validate_pack_response
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError, VirtualSessionIdentity

from .capability_composition import compose_capability_views

PACK_DEFINITION = CapabilityDefinition("memory.pack", ProfileResourceKind.DURABLE_MEMORY,
                                       EffectClass.READ_ONLY, ApprovalMode.NONE)


class MemoryPackUnavailable(RuntimeError):
    pass


class MemoryPackService:
    """One immutable profile binding; no discovery, provider startup or Store."""

    def __init__(self, *, registry, agent_id, resource_id, scope, grants, pack):
        if type(registry) is not AgentProfileRegistry:
            raise ContractError("memory profile registry invalid")
        profile = registry.profile(agent_id)
        if profile.durable_memory_ref != resource_id:
            raise ContractError("memory resource differs from profile")
        _text(scope, 256)
        if not callable(pack):
            raise ContractError("memory Pack callable unavailable")
        self._view = compose_capability_views(
            profile_registry=registry, definitions=(PACK_DEFINITION,), grants=grants,
        ).view(agent_id)
        self._agent_id, self._resource_id, self._scope = agent_id, resource_id, scope
        self._pack = pack

    def pack(self, session, query, *, budget_tokens=1200, max_items=8):
        if type(session) is not VirtualSessionIdentity or session.agent_id != self._agent_id:
            raise ContractError("memory session owner mismatch")
        decision = self._view.decide("memory.pack", self._resource_id, EffectClass.READ_ONLY)
        if decision.kind is not DecisionKind.ALLOW:
            raise ContractError("memory Pack not granted")
        _text(query, 4096)
        if query != query.strip():
            raise ContractError("memory query must be trimmed")
        if (type(budget_tokens) is not int or not 1 <= budget_tokens <= 16384
                or type(max_items) is not int or not 1 <= max_items <= 64):
            raise ContractError("memory Pack request bounds invalid")
        try:
            value = self._pack(
                query, scope=self._scope, budget_tokens=budget_tokens, limit=max_items,
                vector_backend="python", use_tracking=False, quota_enabled=False,
                quota_preference_min=0, quota_decision_min=0, quota_event_max_ratio=1.0,
                scoring_profile="relevance", scoring_relevance_enabled=True,
                scoring_importance_enabled=False, scoring_recency_enabled=False,
                scoring_use_enabled=False, scoring_state_enabled=False,
            )
            return validate_pack_response(value, query=query, scope=self._scope,
                                          budget_tokens=budget_tokens, max_items=max_items)
        except Exception:
            raise MemoryPackUnavailable("memory Pack result unavailable") from None
