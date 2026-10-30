"""Immutable full Linux backend identity and retirement proof values."""

import re
from dataclasses import dataclass
from uuid import UUID

from .sessions import ContractError


@dataclass(frozen=True, slots=True)
class LinuxUnitIdentity:
    distribution: str
    boot_id: str
    unit: str
    invocation_id: str

    def __post_init__(self):
        if type(self.distribution) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", self.distribution
        ):
            raise ContractError("backend distribution invalid")
        try:
            if str(UUID(self.boot_id)) != self.boot_id:
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise ContractError("backend boot identity invalid") from None
        if type(self.unit) is not str or not re.fullmatch(
            r"ahc-backend-[0-9a-f]{32}\.service", self.unit
        ):
            raise ContractError("backend unit outside owned namespace")
        if (
            type(self.invocation_id) is not str
            or not re.fullmatch(r"[0-9a-f]{32}", self.invocation_id)
            or self.invocation_id == "0" * 32
        ):
            raise ContractError("backend invocation identity invalid")


@dataclass(frozen=True, slots=True)
class UnitObservation:
    distribution: str
    boot_id: str
    unit: str
    invocation_id: str | None
    state: str
    cgroup_empty: bool | None


@dataclass(frozen=True, slots=True)
class UnitRetirement:
    identity: LinuxUnitIdentity
    current_boot_id: str

    def __post_init__(self):
        if type(self.identity) is not LinuxUnitIdentity:
            raise ContractError("retirement identity invalid")
        try:
            if (
                str(UUID(self.current_boot_id)) != self.current_boot_id
                or self.current_boot_id == self.identity.boot_id
            ):
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise ContractError("retirement requires a different valid boot") from None
