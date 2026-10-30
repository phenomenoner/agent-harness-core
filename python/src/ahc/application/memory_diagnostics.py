"""Observe an explicitly composed unavailable memory port without discovery."""

from ahc.domain.memory_availability import MemoryAvailability
from ahc.domain.sessions import ContractError


def inspect_memory(port):
    if port is None:
        return {"status": "not-inspected", "reason_code": "memory-not-inspected"}
    availability = port.status()
    if type(availability) is not MemoryAvailability:
        raise ContractError("memory availability observation invalid")
    return availability.to_dict()
