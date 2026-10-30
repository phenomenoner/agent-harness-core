"""Immutable, strict virtual-session identity contracts for M1."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

VIRTUAL_SESSION_REF_SCHEMA_VERSION = "ahc.virtual-session-ref.v1"
VIRTUAL_SESSION_SCHEMA_VERSION = "ahc.virtual-session.v1"


class ContractError(ValueError):
    """A public domain value does not satisfy its frozen contract."""


def _exact_mapping(value: object, expected: frozenset[str], label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ContractError(f"{label} must be an object mapping")
    try:
        actual = set(value.keys())
    except (AttributeError, TypeError) as exc:
        raise ContractError(f"{label} must have string object keys") from exc
    if actual != expected:
        missing = sorted(expected - actual, key=repr)
        unknown = sorted(actual - expected, key=repr)
        details: list[str] = []
        if missing:
            details.append(f"missing keys {missing!r}")
        if unknown:
            details.append(f"unknown keys {unknown!r}")
        raise ContractError(f"{label} has invalid keys: {', '.join(details)}")
    return value


def _schema_version(value: object, expected: str, label: str) -> None:
    if type(value) is not str or value != expected:
        raise ContractError(f"{label}.schema_version must be {expected!r}")


def _identifier(value: object, label: str) -> str:
    if type(value) is not str:
        raise ContractError(f"{label} must be an ASCII identifier string")
    if not 1 <= len(value) <= 128:
        raise ContractError(f"{label} must contain 1 to 128 characters")
    if not _ascii_alphanumeric(value[0]):
        raise ContractError(f"{label} must begin with an ASCII alphanumeric character")
    allowed = set("._:-")
    if any(not _ascii_alphanumeric(char) and char not in allowed for char in value[1:]):
        raise ContractError(f"{label} contains an invalid character")
    return value


def _ascii_alphanumeric(value: str) -> bool:
    return ("a" <= value <= "z") or ("A" <= value <= "Z") or ("0" <= value <= "9")


def _generation(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ContractError(f"{label} must be a strict positive integer")
    return value


@dataclass(frozen=True, slots=True)
class VirtualSessionRef:
    """The immutable session identifier and current positive generation."""

    schema_version: str
    virtual_session_id: str
    generation: int

    def __post_init__(self) -> None:
        _schema_version(self.schema_version, VIRTUAL_SESSION_REF_SCHEMA_VERSION, "ref")
        _identifier(self.virtual_session_id, "virtual_session_id")
        _generation(self.generation, "generation")

    @classmethod
    def initial(cls, virtual_session_id: str) -> VirtualSessionRef:
        """Construct generation one without introducing a second identity shape."""

        return cls(VIRTUAL_SESSION_REF_SCHEMA_VERSION, virtual_session_id, 1)

    def successor(self) -> VirtualSessionRef:
        """Return the next generation for the same virtual session."""

        return type(self)(
            VIRTUAL_SESSION_REF_SCHEMA_VERSION,
            self.virtual_session_id,
            self.generation + 1,
        )

    def to_dict(self) -> dict[str, object]:
        """Emit a fresh JSON-safe exact-key mapping."""

        return {
            "schema_version": self.schema_version,
            "virtual_session_id": self.virtual_session_id,
            "generation": self.generation,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> VirtualSessionRef:
        """Parse one exact-key JSON-shaped mapping without coercion."""

        raw = _exact_mapping(
            value,
            frozenset({"schema_version", "virtual_session_id", "generation"}),
            "virtual session ref",
        )
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["virtual_session_id"]),
            cast(int, raw["generation"]),
        )


@dataclass(frozen=True, slots=True)
class VirtualSessionIdentity:
    """An AHC-owned session generation with explicit agent, route, and lineage."""

    schema_version: str
    ref: VirtualSessionRef
    agent_id: str
    route_id: str
    predecessor: VirtualSessionRef | None

    def __post_init__(self) -> None:
        _schema_version(self.schema_version, VIRTUAL_SESSION_SCHEMA_VERSION, "session")
        if type(self.ref) is not VirtualSessionRef:
            raise ContractError("ref must be a VirtualSessionRef")
        _identifier(self.agent_id, "agent_id")
        _identifier(self.route_id, "route_id")
        if self.predecessor is not None and type(self.predecessor) is not VirtualSessionRef:
            raise ContractError("predecessor must be null or a VirtualSessionRef")
        if self.ref.generation == 1:
            if self.predecessor is not None:
                raise ContractError("generation 1 must not have a predecessor")
        elif self.predecessor is None:
            raise ContractError("generation greater than 1 requires a predecessor")
        elif self.predecessor.virtual_session_id != self.ref.virtual_session_id:
            raise ContractError("predecessor must bind the same virtual_session_id")
        elif self.predecessor.generation != self.ref.generation - 1:
            raise ContractError("predecessor generation must be current generation minus one")

    @classmethod
    def initial(
        cls, virtual_session_id: str, agent_id: str, route_id: str
    ) -> VirtualSessionIdentity:
        """Construct a valid generation-one identity."""

        return cls(
            VIRTUAL_SESSION_SCHEMA_VERSION,
            VirtualSessionRef.initial(virtual_session_id),
            agent_id,
            route_id,
            None,
        )

    def successor(
        self, *, agent_id: str | None = None, route_id: str | None = None
    ) -> VirtualSessionIdentity:
        """Construct the exact next generation, retaining the current ref as predecessor."""

        return type(self)(
            VIRTUAL_SESSION_SCHEMA_VERSION,
            self.ref.successor(),
            self.agent_id if agent_id is None else agent_id,
            self.route_id if route_id is None else route_id,
            self.ref,
        )

    def to_dict(self) -> dict[str, object]:
        """Emit a fresh JSON-safe exact-key mapping."""

        return {
            "schema_version": self.schema_version,
            "ref": self.ref.to_dict(),
            "agent_id": self.agent_id,
            "route_id": self.route_id,
            "predecessor": None if self.predecessor is None else self.predecessor.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> VirtualSessionIdentity:
        """Parse one exact-key mapping and validate lineage after nested values."""

        raw = _exact_mapping(
            value,
            frozenset({"schema_version", "ref", "agent_id", "route_id", "predecessor"}),
            "virtual session identity",
        )
        try:
            ref = VirtualSessionRef.from_dict(cast(Mapping[str, object], raw["ref"]))
        except ContractError as exc:
            raise ContractError(f"ref: {exc}") from exc
        predecessor_value = raw["predecessor"]
        if predecessor_value is None:
            predecessor = None
        else:
            try:
                predecessor = VirtualSessionRef.from_dict(
                    cast(Mapping[str, object], predecessor_value)
                )
            except ContractError as exc:
                raise ContractError(f"predecessor: {exc}") from exc
        return cls(
            cast(str, raw["schema_version"]),
            ref,
            cast(str, raw["agent_id"]),
            cast(str, raw["route_id"]),
            predecessor,
        )


__all__ = [
    "ContractError",
    "VIRTUAL_SESSION_REF_SCHEMA_VERSION",
    "VIRTUAL_SESSION_SCHEMA_VERSION",
    "VirtualSessionIdentity",
    "VirtualSessionRef",
]
