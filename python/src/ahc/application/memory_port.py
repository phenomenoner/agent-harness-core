"""Unavailable-memory seam; the full provider contract is not yet admitted."""

from __future__ import annotations

from typing import Protocol

from ahc.domain.memory_availability import MemoryAvailability, MemoryUnavailableOutcome


class UnavailableMemoryOperations(Protocol):
    """Memory operations stay typed when no provider is configured."""

    def status(self) -> MemoryAvailability: ...

    def read(self, request: object = None) -> MemoryUnavailableOutcome: ...

    def store(self, request: object = None) -> MemoryUnavailableOutcome: ...

    def pack(self, request: object = None) -> MemoryUnavailableOutcome: ...


__all__ = ["UnavailableMemoryOperations"]
