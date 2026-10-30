"""Pure sanitized doctor projection for M2.3-Lite."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ahc.domain.sessions import ContractError, _identifier


class CapabilityPolicyStatus(StrEnum):
    CONFIGURED = "configured"
    NOT_CONFIGURED = "not-configured"
    INVALID_POLICY = "invalid-policy"


@dataclass(frozen=True, slots=True)
class AgentCapabilitySnapshot:
    agent_id: str
    configured_count: int
    denied_count: int
    approval_required_count: int

    def __post_init__(self) -> None:
        _identifier(self.agent_id, "agent_id")
        for label, value in (
            ("configured_count", self.configured_count),
            ("denied_count", self.denied_count),
            ("approval_required_count", self.approval_required_count),
        ):
            if type(value) is not int or value < 0:
                raise ContractError(f"{label} must be a non-negative integer")
        if self.approval_required_count > self.configured_count:
            raise ContractError("approval_required_count cannot exceed configured_count")


@dataclass(frozen=True, slots=True)
class CapabilitySnapshot:
    status: CapabilityPolicyStatus
    agents: tuple[AgentCapabilitySnapshot, ...]

    def __post_init__(self) -> None:
        if type(self.status) is not CapabilityPolicyStatus:
            raise ContractError("status must be a CapabilityPolicyStatus")
        if type(self.agents) is not tuple:
            raise ContractError("agents must be a tuple")
        if any(type(item) is not AgentCapabilitySnapshot for item in self.agents):
            raise ContractError("agents must contain only AgentCapabilitySnapshot values")
        agent_ids = [item.agent_id for item in self.agents]
        if len(set(agent_ids)) != len(agent_ids):
            raise ContractError("doctor snapshot contains duplicate agents")


def inspect_capability_authority(snapshot: CapabilitySnapshot | None) -> dict[str, object]:
    if snapshot is None:
        return {"status": CapabilityPolicyStatus.NOT_CONFIGURED.value, "agents": []}
    if type(snapshot) is not CapabilitySnapshot:
        raise ContractError("snapshot must be a CapabilitySnapshot or None")
    return {
        "status": snapshot.status.value,
        "agents": [
            {
                "agent_id": item.agent_id,
                "configured_count": item.configured_count,
                "denied_count": item.denied_count,
                "approval_required_count": item.approval_required_count,
            }
            for item in snapshot.agents
        ],
    }


__all__ = [
    "AgentCapabilitySnapshot",
    "CapabilityPolicyStatus",
    "CapabilitySnapshot",
    "inspect_capability_authority",
]
