"""Replaceable agent execution seam, independent of vendor and transport types."""

from typing import Protocol

from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest


class AgentBackend(Protocol):
    def invoke(self, request: AgentTurnRequest) -> AgentTurnObservation:
        """Observe one delegated turn; do not accept an AHC task or deliver output."""
        ...
