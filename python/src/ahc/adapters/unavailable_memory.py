"""Explicit no-provider memory adapter with no discovery or lifecycle effects."""

from __future__ import annotations

from ahc.application.memory_port import UnavailableMemoryOperations
from ahc.domain.memory_availability import MemoryAvailability, MemoryUnavailableOutcome
from ahc.domain.sessions import ContractError


class UnavailableMemoryPort(UnavailableMemoryOperations):
    """Return the configured unavailable status for every memory operation."""

    def __init__(self, availability: MemoryAvailability) -> None:
        if type(availability) is not MemoryAvailability:
            raise ContractError("memory port requires an explicit MemoryAvailability")
        self._availability = availability

    def status(self) -> MemoryAvailability:
        return self._availability

    def read(self, request: object = None) -> MemoryUnavailableOutcome:
        return self._outcome("read")

    def store(self, request: object = None) -> MemoryUnavailableOutcome:
        return self._outcome("store")

    def pack(self, request: object = None) -> MemoryUnavailableOutcome:
        return self._outcome("pack")

    def _outcome(self, operation: str) -> MemoryUnavailableOutcome:
        return MemoryUnavailableOutcome(operation, self._availability)


__all__ = ["UnavailableMemoryPort"]
