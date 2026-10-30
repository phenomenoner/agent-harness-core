"""Immutable, secret-free M2.5 credential-use control values."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from .sessions import ContractError, _exact_mapping, _identifier, _schema_version

CREDENTIAL_USE_REQUEST_SCHEMA_VERSION = "ahc.credential-use-request.v1"
CREDENTIAL_USE_OUTCOME_SCHEMA_VERSION = "ahc.credential-use-outcome.v1"


class CredentialPurpose(StrEnum):
    MODEL_INVOKE = "model.invoke"
    DURABLE_MEMORY_PACK = "durable-memory.pack"
    DURABLE_MEMORY_OBSERVE = "durable-memory.observe"
    DURABLE_MEMORY_STORE = "durable-memory.store"
    WORKSPACE_INVOKE = "workspace.invoke"
    CHANNEL_DELIVER = "channel.deliver"
    CHILD_MINION_INVOKE = "child.minion.invoke"
    MCP_INVOKE = "mcp.invoke"


class CredentialAdmissionDecision(StrEnum):
    ALLOW = "allow"
    DENY = "deny"


class CredentialUseStatus(StrEnum):
    USED = "used"
    DENIED = "denied"
    UNAVAILABLE = "unavailable"
    ERROR = "error"
    CANCELLED = "cancelled"


_REASON_CODES = {
    CredentialUseStatus.USED: frozenset({"consumer-returned"}),
    CredentialUseStatus.DENIED: frozenset(
        {
            "profile-binding-denied",
            "purpose-denied",
            "domain-denied",
            "domain-admission-failed",
            "attempt-in-flight",
        }
    ),
    CredentialUseStatus.UNAVAILABLE: frozenset({"provider-unavailable"}),
    CredentialUseStatus.ERROR: frozenset(
        {
            "provider-error",
            "provider-contract-error",
            "consumer-error",
            "consumer-result-unsafe",
            "cancellation-probe-error",
        }
    ),
    CredentialUseStatus.CANCELLED: frozenset({"cancelled-before-consumer", "consumer-cancelled"}),
}
_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "agent_id",
        "backend_profile_ref",
        "credential_ref",
        "purpose",
        "operation_id",
        "attempt_id",
    }
)
_OUTCOME_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "reason_code",
        "agent_id",
        "purpose",
        "operation_id",
        "attempt_id",
        "binding_digest",
    }
)


def _safe_exact_mapping(
    value: Mapping[str, object], fields: frozenset[str], label: str
) -> Mapping[str, object]:
    invalid = False
    raw: Mapping[str, object] | None = None
    try:
        raw = _exact_mapping(value, fields, label)
    except ContractError:
        invalid = True
    if invalid:
        raise ContractError(f"{label} has invalid fields")
    assert raw is not None
    return raw


@dataclass(frozen=True, slots=True)
class CredentialUseRequest:
    schema_version: str
    agent_id: str
    backend_profile_ref: str
    credential_ref: str
    purpose: str
    operation_id: str
    attempt_id: str

    def __post_init__(self) -> None:
        _schema_version(
            self.schema_version,
            CREDENTIAL_USE_REQUEST_SCHEMA_VERSION,
            "credential use request",
        )
        for field_name in (
            "agent_id",
            "backend_profile_ref",
            "credential_ref",
            "purpose",
            "operation_id",
            "attempt_id",
        ):
            _identifier(getattr(self, field_name), field_name)

    @property
    def binding_digest(self) -> str:
        values = (
            self.schema_version,
            self.agent_id,
            self.backend_profile_ref,
            self.credential_ref,
            self.purpose,
            self.operation_id,
            self.attempt_id,
        )
        canonical = bytearray()
        for value in values:
            encoded = value.encode("utf-8")
            canonical.extend(len(encoded).to_bytes(8, "big"))
            canonical.extend(encoded)
        return f"sha256:{hashlib.sha256(canonical).hexdigest()}"

    def to_dict(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "agent_id": self.agent_id,
            "backend_profile_ref": self.backend_profile_ref,
            "credential_ref": self.credential_ref,
            "purpose": self.purpose,
            "operation_id": self.operation_id,
            "attempt_id": self.attempt_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CredentialUseRequest:
        raw = _safe_exact_mapping(value, _REQUEST_FIELDS, "credential use request")
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["agent_id"]),
            cast(str, raw["backend_profile_ref"]),
            cast(str, raw["credential_ref"]),
            cast(str, raw["purpose"]),
            cast(str, raw["operation_id"]),
            cast(str, raw["attempt_id"]),
        )


@dataclass(frozen=True, slots=True)
class CredentialUseOutcome:
    schema_version: str
    status: CredentialUseStatus
    reason_code: str
    agent_id: str
    purpose: str
    operation_id: str
    attempt_id: str
    binding_digest: str

    def __post_init__(self) -> None:
        _schema_version(
            self.schema_version,
            CREDENTIAL_USE_OUTCOME_SCHEMA_VERSION,
            "credential use outcome",
        )
        if type(self.status) is not CredentialUseStatus:
            raise ContractError("status must be a CredentialUseStatus")
        _identifier(self.reason_code, "reason_code")
        if self.reason_code not in _REASON_CODES[self.status]:
            raise ContractError("reason_code is inconsistent with credential use status")
        for field_name in ("agent_id", "purpose", "operation_id", "attempt_id"):
            _identifier(getattr(self, field_name), field_name)
        if (
            type(self.binding_digest) is not str
            or not self.binding_digest.startswith("sha256:")
            or len(self.binding_digest) != 71
        ):
            raise ContractError("binding_digest must be a SHA-256 digest")
        if any(char not in "0123456789abcdef" for char in self.binding_digest[7:]):
            raise ContractError("binding_digest must contain lowercase hexadecimal")

    def to_dict(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "status": self.status.value,
            "reason_code": self.reason_code,
            "agent_id": self.agent_id,
            "purpose": self.purpose,
            "operation_id": self.operation_id,
            "attempt_id": self.attempt_id,
            "binding_digest": self.binding_digest,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CredentialUseOutcome:
        raw = _safe_exact_mapping(value, _OUTCOME_FIELDS, "credential use outcome")
        invalid_status = False
        status: CredentialUseStatus | None = None
        try:
            status = CredentialUseStatus(raw["status"])
        except (TypeError, ValueError):
            invalid_status = True
        if invalid_status:
            raise ContractError("credential use outcome status is unsupported")
        assert status is not None
        return cls(
            cast(str, raw["schema_version"]),
            status,
            cast(str, raw["reason_code"]),
            cast(str, raw["agent_id"]),
            cast(str, raw["purpose"]),
            cast(str, raw["operation_id"]),
            cast(str, raw["attempt_id"]),
            cast(str, raw["binding_digest"]),
        )


__all__ = [
    "CREDENTIAL_USE_OUTCOME_SCHEMA_VERSION",
    "CREDENTIAL_USE_REQUEST_SCHEMA_VERSION",
    "CredentialAdmissionDecision",
    "CredentialPurpose",
    "CredentialUseOutcome",
    "CredentialUseRequest",
    "CredentialUseStatus",
]
