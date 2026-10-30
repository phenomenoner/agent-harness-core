"""Profile-authorized public lexical search; no exact-record Observe claim."""

from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDefinition,
    DecisionKind,
    EffectClass,
)
from ahc.domain.memory_pack import _text
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import canonical_json

from .capability_composition import compose_capability_views


class MemorySearchUnavailable(RuntimeError):
    pass


class MemorySearchService:
    def __init__(self, *, registry, agent_id, resource_id, scope, grants, search):
        if type(registry) is not AgentProfileRegistry:
            raise ContractError("memory search registry invalid")
        profile = registry.profile(agent_id)
        if profile.durable_memory_ref != resource_id or not callable(search):
            raise ContractError("memory search binding invalid")
        _text(scope, 256)
        definition = CapabilityDefinition("memory.search", ProfileResourceKind.DURABLE_MEMORY,
                                          EffectClass.READ_ONLY, ApprovalMode.NONE)
        self._view = compose_capability_views(
            profile_registry=registry, definitions=(definition,), grants=grants).view(agent_id)
        self._agent, self._resource, self._scope = agent_id, resource_id, scope
        self._search = search

    def search(self, session, query, *, limit=8):
        if (type(session) is not VirtualSessionIdentity or session.agent_id != self._agent
                or self._view.decide("memory.search", self._resource,
                                     EffectClass.READ_ONLY).kind is not DecisionKind.ALLOW):
            raise ContractError("memory search not admitted")
        _text(query, 4096)
        if query != query.strip() or type(limit) is not int or not 1 <= limit <= 64:
            raise ContractError("memory search bounds invalid")
        try:
            rows = self._search(query, scope=self._scope, limit=limit, include_archived=False,
                                scoring_profile="relevance", scoring_relevance_enabled=True,
                                scoring_importance_enabled=False, scoring_recency_enabled=False,
                                scoring_use_enabled=False, scoring_state_enabled=False)
            if type(rows) is not list or len(rows) > limit:
                raise ValueError("memory search result invalid")
            records, seen = [], set()
            for row in rows:
                if type(row) is not dict or not {"id", "summary"} <= set(row):
                    raise ValueError("memory search record invalid")
                identifier = row["id"]
                if type(identifier) is not int or identifier < 1 or identifier in seen:
                    raise ValueError("memory search identifier invalid")
                _text(row["summary"], 16384, empty=True)
                seen.add(identifier)
                records.append({"provider_id": identifier, "summary": row["summary"]})
            result = {"query": query, "requested_scope": self._scope, "records": records,
                      "content_authority": "untrusted-data", "trust_source": "provider-asserted",
                      "trace_coverage": "not-established", "complete_recall": False}
            if len(canonical_json(result)) > 131072:
                raise ValueError("memory search response too large")
            return result
        except Exception:
            raise MemorySearchUnavailable("memory search unavailable") from None
