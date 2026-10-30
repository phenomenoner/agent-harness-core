"""Strict typed fake-turn request and result contracts for M1."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import cast

from .sessions import (
    ContractError,
    VirtualSessionIdentity,
    _exact_mapping,
    _identifier,
    _schema_version,
)

FAKE_TURN_REQUEST_SCHEMA_VERSION = "ahc.fake-turn-request.v1"
FAKE_TURN_RESULT_SCHEMA_VERSION = "ahc.fake-turn-result.v1"
FAKE_TURN_STATUS = "completed"
FAKE_TURN_ENGINE = "offline-fake"
FAKE_TURN_OUTPUT_PREFIX = "FAKE: "


def _input_text(value: object, label: str = "input_text") -> str:
    if type(value) is not str:
        raise ContractError(f"{label} must be a string")
    if not value or value.strip() == "":
        raise ContractError(f"{label} must not be blank")
    if len(value) > 4096:
        raise ContractError(f"{label} must contain at most 4096 Unicode scalar values")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ContractError(f"{label} must contain only Unicode scalar values")
    return value


def _sha256_hex(value: object) -> str:
    if type(value) is not str or len(value) != 64:
        raise ContractError("input_sha256 must be a lowercase SHA-256 hex string")
    if any(character not in "0123456789abcdef" for character in value):
        raise ContractError("input_sha256 must be a lowercase SHA-256 hex string")
    return value


@dataclass(frozen=True, slots=True)
class FakeTurnRequest:
    """A user turn bound to one complete virtual-session identity."""

    schema_version: str
    session: VirtualSessionIdentity
    turn_id: str
    input_text: str

    def __post_init__(self) -> None:
        _schema_version(self.schema_version, FAKE_TURN_REQUEST_SCHEMA_VERSION, "fake turn request")
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("session must be a VirtualSessionIdentity")
        _identifier(self.turn_id, "turn_id")
        _input_text(self.input_text)

    @property
    def session_identity(self) -> VirtualSessionIdentity:
        """Readable alias for callers that name the nested value explicitly."""

        return self.session

    def to_dict(self) -> dict[str, object]:
        """Emit a fresh JSON-safe exact-key mapping."""

        return {
            "schema_version": self.schema_version,
            "session": self.session.to_dict(),
            "turn_id": self.turn_id,
            "input_text": self.input_text,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> FakeTurnRequest:
        """Parse one exact-key mapping without coercion."""

        raw = _exact_mapping(
            value,
            frozenset({"schema_version", "session", "turn_id", "input_text"}),
            "fake turn request",
        )
        try:
            session = VirtualSessionIdentity.from_dict(cast(Mapping[str, object], raw["session"]))
        except ContractError as exc:
            raise ContractError(f"session: {exc}") from exc
        return cls(
            cast(str, raw["schema_version"]),
            session,
            cast(str, raw["turn_id"]),
            cast(str, raw["input_text"]),
        )


@dataclass(frozen=True, slots=True)
class FakeTurnResult:
    """The deterministic completed result for one fake-turn request."""

    schema_version: str
    session: VirtualSessionIdentity
    turn_id: str
    status: str
    engine: str
    output_text: str
    input_sha256: str

    def __post_init__(self) -> None:
        _schema_version(self.schema_version, FAKE_TURN_RESULT_SCHEMA_VERSION, "fake turn result")
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("session must be a VirtualSessionIdentity")
        _identifier(self.turn_id, "turn_id")
        if type(self.status) is not str or self.status != FAKE_TURN_STATUS:
            raise ContractError(f"status must be {FAKE_TURN_STATUS!r}")
        if type(self.engine) is not str or self.engine != FAKE_TURN_ENGINE:
            raise ContractError(f"engine must be {FAKE_TURN_ENGINE!r}")
        if type(self.output_text) is not str or not self.output_text.startswith(
            FAKE_TURN_OUTPUT_PREFIX
        ):
            raise ContractError("output_text must start with the deterministic fake-turn prefix")
        output_input = _input_text(self.output_text[len(FAKE_TURN_OUTPUT_PREFIX) :], "output_text")
        input_sha256 = _sha256_hex(self.input_sha256)
        if input_sha256 != sha256(output_input.encode("utf-8")).hexdigest():
            raise ContractError("input_sha256 does not match output_text input")

    @property
    def session_identity(self) -> VirtualSessionIdentity:
        """Readable alias for callers that name the nested value explicitly."""

        return self.session

    def to_dict(self) -> dict[str, object]:
        """Emit a fresh JSON-safe exact-key mapping."""

        return {
            "schema_version": self.schema_version,
            "session": self.session.to_dict(),
            "turn_id": self.turn_id,
            "status": self.status,
            "engine": self.engine,
            "output_text": self.output_text,
            "input_sha256": self.input_sha256,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> FakeTurnResult:
        """Parse one exact-key mapping without coercion."""

        raw = _exact_mapping(
            value,
            frozenset(
                {
                    "schema_version",
                    "session",
                    "turn_id",
                    "status",
                    "engine",
                    "output_text",
                    "input_sha256",
                }
            ),
            "fake turn result",
        )
        try:
            session = VirtualSessionIdentity.from_dict(cast(Mapping[str, object], raw["session"]))
        except ContractError as exc:
            raise ContractError(f"session: {exc}") from exc
        return cls(
            cast(str, raw["schema_version"]),
            session,
            cast(str, raw["turn_id"]),
            cast(str, raw["status"]),
            cast(str, raw["engine"]),
            cast(str, raw["output_text"]),
            cast(str, raw["input_sha256"]),
        )


__all__ = [
    "FAKE_TURN_ENGINE",
    "FAKE_TURN_OUTPUT_PREFIX",
    "FAKE_TURN_REQUEST_SCHEMA_VERSION",
    "FAKE_TURN_RESULT_SCHEMA_VERSION",
    "FAKE_TURN_STATUS",
    "FakeTurnRequest",
    "FakeTurnResult",
]
