"""AHC-owned workspace generations and portable JSON-subset checkpoints."""

import json
from dataclasses import dataclass

from .sessions import ContractError, VirtualSessionIdentity, _identifier
from .skill_values import digest_json


class CheckpointValueRejected(ContractError):
    """Trusted local value validation failed before any checkpoint was saved."""


class CheckpointNotSaved(ContractError):
    """Capture failed before the checkpoint writer; resource health is separate."""


class WorkspaceOperationNotStarted(ContractError):
    """The runtime rejected admission before invoking the interpreter port."""


def workspace_failure_projection(value):
    """Fixed diagnostic categories only; no exception text or user payload."""
    if type(value) is not dict:
        return {}
    phases = {"admission", "send", "receive", "result-validation", "result-save", "execute"}
    codes = {"writer-busy", "owner-conflict", "io", "contract", "internal", "unknown",
             "deadline", "closed", "write-indeterminate", "eof", "invalid-encoding",
             "malformed-json", "protocol-frame", "reader-io", "cleanup-unverified",
             "invalid-output", "invalid-display", "cancelled"}
    cleanup = {"not-observed", "confirmed", "unverified"}
    return {key: value[key] for key, allowed in (
        ("phase", phases), ("code", codes), ("cleanup", cleanup))
        if type(value.get(key)) is str and value[key] in allowed}


class WorkspaceCodeRejected(ContractError):
    """Code cannot fit the real control request; no execution has started."""

    def __init__(self, request_bytes=None, max_request_bytes=None):
        super().__init__("workspace code encoding or control frame invalid")
        self.request_bytes, self.max_request_bytes = request_bytes, max_request_bytes


@dataclass(frozen=True, slots=True)
class WorkspaceIdentity:
    session: VirtualSessionIdentity
    workspace_id: str
    generation: int = 1
    revision: int = 0

    def __post_init__(self):
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("workspace requires exact session")
        _identifier(self.workspace_id, "workspace")
        if type(self.generation) is not int or self.generation < 1:
            raise ContractError("workspace generation invalid")
        if type(self.revision) is not int or self.revision < 0:
            raise ContractError("workspace revision invalid")

    def to_dict(self):
        return {
            "session": self.session.to_dict(),
            "workspace_id": self.workspace_id,
            "generation": self.generation,
            "revision": self.revision,
        }

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != {
            "session",
            "workspace_id",
            "generation",
            "revision",
        }:
            raise ContractError("workspace identity fields invalid")
        return cls(
            VirtualSessionIdentity.from_dict(value["session"]),
            value["workspace_id"],
            value["generation"],
            value["revision"],
        )


def portable_values(value, depth=0):
    if depth > 8:
        raise ContractError("checkpoint depth exceeded")
    kind = type(value)
    if value is None or kind is bool:
        return value
    if kind is int and value.bit_length() <= 1024:
        return value
    if kind is str and len(value) <= 4096 and not any(0xD800 <= ord(c) <= 0xDFFF for c in value):
        return value
    if kind is list and len(value) <= 128:
        return [portable_values(item, depth + 1) for item in value]
    if (
        kind is dict
        and len(value) <= 128
        and all(
            type(k) is str and len(k) <= 128 and not any(0xD800 <= ord(c) <= 0xDFFF for c in k)
            for k in value
        )
    ):
        return {key: portable_values(item, depth + 1) for key, item in value.items()}
    raise ContractError("checkpoint requires bounded JSON subset")


@dataclass(frozen=True, slots=True)
class WorkspaceCheckpoint:
    origin: WorkspaceIdentity
    values_json: str

    def __post_init__(self):
        if type(self.origin) is not WorkspaceIdentity:
            raise ContractError("checkpoint origin invalid")
        if type(self.values_json) is not str or len(self.values_json) > 32768:
            raise ContractError("checkpoint exceeds bound")
        try:

            def unique_object(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError("duplicate checkpoint key")
                    result[key] = value
                return result

            values = json.loads(self.values_json, object_pairs_hook=unique_object)
        except (ValueError, RecursionError):
            raise ContractError("checkpoint JSON invalid") from None
        if (
            type(values) is not dict
            or len(values) > 32
            or any(
                not key.isascii() or not key.isidentifier() or key.startswith("_") for key in values
            )
        ):
            raise ContractError("checkpoint names invalid")
        portable_values(values)
        canonical = json.dumps(values, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
        if len(canonical) > 32768:
            raise ContractError("canonical checkpoint exceeds bound")
        object.__setattr__(self, "values_json", canonical)

    @property
    def digest(self):
        return digest_json(
            {"origin": self.origin.to_dict(), "values": json.loads(self.values_json)}
        )
