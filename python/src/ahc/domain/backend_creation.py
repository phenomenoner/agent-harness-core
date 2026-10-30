"""Positive refusal before unit creation, distinct from an indeterminate attempt."""

from dataclasses import dataclass

from .backend_identity import LinuxUnitIdentity


@dataclass(frozen=True, slots=True)
class BackendCreationAttempt:
    """Pre-effect correlation key; never authority to stop a process by name."""

    distribution: str
    unit: str

    def __post_init__(self):
        LinuxUnitIdentity(self.distribution, "12345678-1234-1234-1234-123456789abc",
                          self.unit, "1" * 32)


@dataclass(frozen=True, slots=True)
class BackendCreationCancelled:
    """The controller durably fenced this attempt before any create effect."""

    attempt: BackendCreationAttempt


class BackendCreationRejected(RuntimeError):
    """The trusted controller positively refused before invoking unit creation."""

    reason_code = "remote-interpreter-absent"
