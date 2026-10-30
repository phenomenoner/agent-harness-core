"""Secret-free values for the B1 credential-effect boundary."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from .sessions import ContractError

CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION = "ahc.credential-effect-binding-snapshot.v1"
CREDENTIAL_EFFECT_RESPONSE_SCHEMA_VERSION = "ahc.credential-effect-response.v1"
MAX_CREDENTIAL_EFFECT_RESPONSE_BYTES = 8_192

_BINDING_FIELDS = frozenset({"schema_version", "status", "purpose"})
_RESPONSE_FIELDS = frozenset(
    {
        "schema_version",
        "runtime_generation",
        "capsule_generation",
        "operation_id",
        "attempt_id",
        "request_digest",
        "channel_sequence",
        "transport_status",
        "reason_code",
    }
)
_IDENTIFIER_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-")


class CredentialEffectBindingStatus(StrEnum):
    NOT_CONFIGURED = "not-configured"
    INCOMPATIBLE = "incompatible"
    UNAVAILABLE = "unavailable"
    DEAD = "dead"


class CredentialEffectTransportStatus(StrEnum):
    DENIED = "denied"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"
    UNKNOWN = "unknown"
    CANCELLED = "cancelled"


_REASON_CODES: dict[CredentialEffectTransportStatus, frozenset[str]] = {
    CredentialEffectTransportStatus.DENIED: frozenset(
        {"binding-denied", "purpose-denied", "admission-denied", "frame-denied"}
    ),
    CredentialEffectTransportStatus.UNAVAILABLE: frozenset(
        {
            "not-configured",
            "peer-unavailable",
            "provider-unavailable",
            "identity-unavailable",
        }
    ),
    CredentialEffectTransportStatus.FAILED: frozenset(
        {"frame-rejected", "provider-failed", "consumer-failed"}
    ),
    CredentialEffectTransportStatus.UNKNOWN: frozenset(
        {"response-lost", "channel-lost", "effect-unknown"}
    ),
    CredentialEffectTransportStatus.CANCELLED: frozenset({"cancelled-before-write"}),
}


def _exact_dict(value: object, fields: frozenset[str], label: str) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[object, object], value)) != fields:
        raise ContractError(f"{label} has invalid fields")
    return cast(dict[str, object], value)


def _parse_enum[EnumValue: StrEnum](enum: type[EnumValue], value: object, label: str) -> EnumValue:
    if type(value) is not str:
        raise ContractError(f"{label} is unsupported")
    try:
        return enum(value)
    except ValueError as exc:
        raise ContractError(f"{label} is unsupported") from exc


def _identifier(value: object, label: str) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > 128
        or not value.isascii()
        or any(char not in _IDENTIFIER_CHARS for char in value)
    ):
        raise ContractError(f"{label} is invalid")
    return value


def _positive_integer(value: object, label: str) -> int:
    if type(value) is not int or value < 1:
        raise ContractError(f"{label} must be a positive integer")
    return value


def _request_digest(value: object) -> str:
    if (
        type(value) is not str
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(char not in "0123456789abcdef" for char in value[7:])
    ):
        raise ContractError("request_digest must be a lowercase SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class CredentialEffectBindingSnapshot:
    schema_version: str
    status: CredentialEffectBindingStatus
    purpose: str | None

    def __post_init__(self) -> None:
        if self.schema_version != CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION:
            raise ContractError("credential effect binding snapshot schema version is unsupported")
        if type(self.status) is not CredentialEffectBindingStatus:
            raise ContractError("credential effect binding status is unsupported")
        if self.status is CredentialEffectBindingStatus.NOT_CONFIGURED:
            if self.purpose is not None:
                raise ContractError("purpose must be absent when binding is not configured")
        elif self.purpose is None:
            raise ContractError("purpose is required when binding status is known")
        else:
            _identifier(self.purpose, "purpose")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status.value,
            "purpose": self.purpose,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CredentialEffectBindingSnapshot:
        raw = _exact_dict(value, _BINDING_FIELDS, "credential effect binding snapshot")
        return cls(
            cast(str, raw["schema_version"]),
            _parse_enum(
                CredentialEffectBindingStatus,
                raw["status"],
                "credential effect binding status",
            ),
            cast(str | None, raw["purpose"]),
        )


@dataclass(frozen=True, slots=True)
class CredentialEffectResponse:
    schema_version: str
    runtime_generation: int
    capsule_generation: int
    operation_id: str
    attempt_id: str
    request_digest: str
    channel_sequence: int
    transport_status: CredentialEffectTransportStatus
    reason_code: str

    def __post_init__(self) -> None:
        if self.schema_version != CREDENTIAL_EFFECT_RESPONSE_SCHEMA_VERSION:
            raise ContractError("credential effect response schema version is unsupported")
        _positive_integer(self.runtime_generation, "runtime_generation")
        _positive_integer(self.capsule_generation, "capsule_generation")
        _identifier(self.operation_id, "operation_id")
        _identifier(self.attempt_id, "attempt_id")
        _request_digest(self.request_digest)
        _positive_integer(self.channel_sequence, "channel_sequence")
        if type(self.transport_status) is not CredentialEffectTransportStatus:
            raise ContractError("credential effect transport status is unsupported")
        _identifier(self.reason_code, "reason_code")
        if self.reason_code not in _REASON_CODES[self.transport_status]:
            raise ContractError(
                "reason_code is inconsistent with credential effect transport status"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "runtime_generation": self.runtime_generation,
            "capsule_generation": self.capsule_generation,
            "operation_id": self.operation_id,
            "attempt_id": self.attempt_id,
            "request_digest": self.request_digest,
            "channel_sequence": self.channel_sequence,
            "transport_status": self.transport_status.value,
            "reason_code": self.reason_code,
        }

    def to_wire(self) -> bytes:
        encoded = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        if len(encoded) > MAX_CREDENTIAL_EFFECT_RESPONSE_BYTES:
            raise ContractError("credential effect response exceeds the size limit")
        return encoded

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CredentialEffectResponse:
        raw = _exact_dict(value, _RESPONSE_FIELDS, "credential effect response")
        return cls(
            cast(str, raw["schema_version"]),
            _positive_integer(raw["runtime_generation"], "runtime_generation"),
            _positive_integer(raw["capsule_generation"], "capsule_generation"),
            _identifier(raw["operation_id"], "operation_id"),
            _identifier(raw["attempt_id"], "attempt_id"),
            _request_digest(raw["request_digest"]),
            _positive_integer(raw["channel_sequence"], "channel_sequence"),
            _parse_enum(
                CredentialEffectTransportStatus,
                raw["transport_status"],
                "credential effect transport status",
            ),
            _identifier(raw["reason_code"], "reason_code"),
        )

    @classmethod
    def from_wire(cls, frame: bytes) -> CredentialEffectResponse:
        if type(frame) is not bytes:
            raise ContractError("credential effect response frame must be bytes")
        if not frame or len(frame) > MAX_CREDENTIAL_EFFECT_RESPONSE_BYTES:
            raise ContractError("credential effect response frame size is invalid")
        try:
            text = frame.decode("utf-8", errors="strict")
            value = json.loads(
                text,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_json_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise ContractError("credential effect response frame is malformed") from None
        return cls.from_dict(cast(Mapping[str, object], value))


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate object field")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> object:
    raise ValueError("non-finite JSON number")


__all__ = [
    "CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION",
    "CREDENTIAL_EFFECT_RESPONSE_SCHEMA_VERSION",
    "MAX_CREDENTIAL_EFFECT_RESPONSE_BYTES",
    "CredentialEffectBindingSnapshot",
    "CredentialEffectBindingStatus",
    "CredentialEffectResponse",
    "CredentialEffectTransportStatus",
]
