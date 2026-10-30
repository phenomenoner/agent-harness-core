"""Explicit finite per-job limits; these values do not allocate a host budget."""

from dataclasses import dataclass

from .sessions import ContractError


@dataclass(frozen=True, slots=True)
class NativeResourceLimits:
    memory_bytes: int
    pids: int
    cpu_quota_us: int
    cpu_period_us: int = 100000
    swap_bytes: int = 0

    def __post_init__(self):
        if (
            any(type(value) is not int for value in (
                self.memory_bytes, self.pids, self.cpu_quota_us,
                self.cpu_period_us, self.swap_bytes))
            or not 16 * 1024**2 <= self.memory_bytes <= 1024**4
            or self.memory_bytes % 65536
            or not 0 <= self.swap_bytes <= self.memory_bytes or self.swap_bytes % 65536
            or not 1 <= self.pids <= 4096
            or not 1000 <= self.cpu_period_us <= 1000000
            or not 1000 <= self.cpu_quota_us <= 64 * self.cpu_period_us
        ):
            raise ContractError("native resource limits require finite explicit values")

    def kernel_values(self):
        return (
            ("memory.max", str(self.memory_bytes)),
            ("memory.swap.max", str(self.swap_bytes)),
            ("pids.max", str(self.pids)),
            ("cpu.max", f"{self.cpu_quota_us} {self.cpu_period_us}"),
        )
