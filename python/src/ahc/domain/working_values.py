"""Pure bounded values for M2.2 working continuity."""

from __future__ import annotations

from .sessions import ContractError, _identifier
from .skill_values import digest_json, optional_digest, text

WORKING_SET_SCHEMA_VERSION = "ahc.working-set.v1"
WORKING_CONTINUITY_STATUS_SCHEMA_VERSION = "ahc.working-continuity-status.v1"
WORKING_SET_STATUSES = frozenset({"active", "interrupted", "cancelled", "completed"})
INTERRUPTION_DISPOSITIONS = frozenset({"operator", "process-loss", "timeout"})
CONTINUATION_REASONS = frozenset({"compaction", "backend-rollover", "restore"})


def positive_revision(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise ContractError("working-set revision must be a strict positive integer")
    return value


def progress_items(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not 0 <= len(value) <= 32:
        raise ContractError("progress must contain 0 to 32 items")
    return tuple(text(item, "progress item", 512, multiline=True) for item in value)


def evidence_refs(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not 0 <= len(value) <= 64:
        raise ContractError("evidence_refs must contain 0 to 64 items")
    refs = tuple(_identifier(item, "evidence_refs item") for item in value)
    if len(refs) != len(set(refs)):
        raise ContractError("evidence_refs must not contain duplicates")
    return refs


def optional_identifier(value: object, label: str) -> str | None:
    return None if value is None else _identifier(value, label)


def optional_choice(value: object, choices: frozenset[str], label: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str or value not in choices:
        raise ContractError(f"{label} has an unsupported value")
    return value


def snapshot_digest(payload: dict[str, object], supplied: object) -> str:
    actual = digest_json(payload)
    expected = optional_digest(supplied, "snapshot_digest")
    if expected is not None and expected != actual:
        raise ContractError("snapshot_digest does not match canonical content")
    return actual


__all__ = [
    "CONTINUATION_REASONS",
    "INTERRUPTION_DISPOSITIONS",
    "WORKING_CONTINUITY_STATUS_SCHEMA_VERSION",
    "WORKING_SET_SCHEMA_VERSION",
    "WORKING_SET_STATUSES",
]
