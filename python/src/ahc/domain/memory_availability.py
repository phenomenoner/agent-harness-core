"""Immutable status-only contracts for an unconfigured memory adapter."""

from __future__ import annotations

from dataclasses import dataclass

from .sessions import ContractError

MEMORY_AVAILABILITY_STATUSES = frozenset({"absent", "disabled", "incompatible", "unavailable"})
_REASON_CODES = {
    "absent": "memory-absent",
    "disabled": "memory-disabled",
    "incompatible": "memory-incompatible",
    "unavailable": "memory-unavailable",
}
_OPERATIONS = frozenset({"read", "store", "pack"})


@dataclass(frozen=True, slots=True)
class MemoryAvailability:
    """A configured observation that does not inspect or initialize a provider."""

    status: str

    def __post_init__(self) -> None:
        if type(self.status) is not str or self.status not in MEMORY_AVAILABILITY_STATUSES:
            raise ContractError("memory availability status is unsupported")

    @property
    def reason_code(self) -> str:
        return _REASON_CODES[self.status]

    def to_dict(self) -> dict[str, str]:
        return {"status": self.status, "reason_code": self.reason_code}


@dataclass(frozen=True, slots=True)
class MemoryUnavailableOutcome:
    """One operation that did not perform a memory effect."""

    operation: str
    availability: MemoryAvailability

    def __post_init__(self) -> None:
        if type(self.operation) is not str or self.operation not in _OPERATIONS:
            raise ContractError("memory operation is unsupported")
        if type(self.availability) is not MemoryAvailability:
            raise ContractError("memory outcome requires a MemoryAvailability")

    @property
    def status(self) -> str:
        return self.availability.status

    @property
    def reason_code(self) -> str:
        return self.availability.reason_code

    def to_dict(self) -> dict[str, str]:
        return {
            "outcome": "unavailable",
            "operation": self.operation,
            "status": self.status,
            "reason_code": self.reason_code,
        }


__all__ = [
    "MEMORY_AVAILABILITY_STATUSES",
    "MemoryAvailability",
    "MemoryUnavailableOutcome",
]
