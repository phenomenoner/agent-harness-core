"""Finite tmpfs capacity for an ephemeral evaluation workspace and its /tmp."""

from dataclasses import dataclass

from .sessions import ContractError


@dataclass(frozen=True, slots=True)
class NativeScratchLimits:
    workspace_bytes: int
    temporary_bytes: int

    def __post_init__(self):
        if any(type(value) is not int or not 1024**2 <= value <= 1024**3 or value % 65536
               for value in (self.workspace_bytes, self.temporary_bytes)):
            raise ContractError("native scratch limits require finite aligned capacities")
