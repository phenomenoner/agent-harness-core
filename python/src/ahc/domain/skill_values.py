"""Small pure validators and canonicalization helpers for ASI values."""

from __future__ import annotations

import json
import re
import unicodedata
from hashlib import sha256
from typing import Any

from .sessions import ContractError

SKILL_REVISION_SCHEMA_VERSION = "ahc.skill-revision.v1"
SKILL_MANIFEST_SCHEMA_VERSION = "ahc.skill-manifest.v1"
SKILL_EPISODE_SCHEMA_VERSION = "ahc.skill-episode.v1"
SKILL_PROPOSAL_SCHEMA_VERSION = "ahc.skill-proposal.v1"
SKILL_EVALUATION_SCHEMA_VERSION = "ahc.skill-evaluation.v1"
SKILL_RELATION_KINDS = frozenset(
    {"depends_on", "complements", "conflicts_with", "variant_of", "replaces"}
)
COGNITIVE_LAYERS = ("why-judgment", "what-strategy", "how-procedure")
LAYER_RANK = {name: index for index, name in enumerate(COGNITIVE_LAYERS)}
OUTCOMES = ("unknown", "verified-success", "verified-failure", "corrected")

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_ROLE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{0,63}$")
_SKILL_RE = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_VERSION_RE = re.compile(r"^(0|[1-9][0-9]{0,3})\.(0|[1-9][0-9]{0,3})\.(0|[1-9][0-9]{0,3})$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ContractError("value is not canonical JSON") from exc


def digest_json(value: Any) -> str:
    return sha256(canonical_json(value)).hexdigest()


def digest_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def exact_mapping(value: object, expected: frozenset[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ContractError(f"{label} must be an object mapping")
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        detail = []
        if missing:
            detail.append(f"missing keys {missing!r}")
        if unknown:
            detail.append(f"unknown keys {unknown!r}")
        raise ContractError(f"{label} has invalid keys: {', '.join(detail)}")
    if any(type(key) is not str for key in value):
        raise ContractError(f"{label} keys must be strings")
    return value


def schema(value: object, expected: str, label: str) -> None:
    if type(value) is not str or value != expected:
        raise ContractError(f"{label}.schema_version must be {expected!r}")


def text(value: object, label: str, maximum: int, *, multiline: bool = False) -> str:
    if type(value) is not str or not value:
        raise ContractError(f"{label} must be non-empty text")
    if len(value) > maximum:
        raise ContractError(f"{label} exceeds {maximum} characters")
    if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
        raise ContractError(f"{label} contains an unpaired surrogate")
    allowed = {"\n", "\t"} if multiline else set()
    if any(
        (ord(char) < 0x20 or 0x7F <= ord(char) <= 0x9F) and char not in allowed for char in value
    ):
        raise ContractError(f"{label} contains a control character")
    if value != value.strip():
        raise ContractError(f"{label} must not have leading or trailing whitespace")
    return value


def normalize_body(value: object) -> str:
    if type(value) is not str or not value:
        raise ContractError("body must be non-empty text")
    if len(value) > 49_152:
        raise ContractError("body exceeds 49152 characters")
    if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
        raise ContractError("body contains an unpaired surrogate")
    if any(
        (ord(char) < 0x20 or 0x7F <= ord(char) <= 0x9F) and char not in {"\n", "\t", "\r"}
        for char in value
    ):
        raise ContractError("body contains a control character")
    if value[0] in " \t\n\r" or value.rstrip("\r\n").endswith((" ", "\t")):
        raise ContractError("body must not have leading or trailing horizontal whitespace")
    body = unicodedata.normalize("NFKC", value).replace("\r\n", "\n").replace("\r", "\n")
    body = body.rstrip("\n") + "\n"
    encoded = body.encode("utf-8")
    if not encoded or len(encoded) > 49_152:
        raise ContractError("body must contain 1 to 49152 UTF-8 bytes")
    return body


def identifier(value: object, label: str, *, role: bool = False) -> str:
    if type(value) is not str or not (1 <= len(value) <= 64):
        raise ContractError(f"{label} must contain 1 to 64 characters")
    if value != value.strip():
        raise ContractError(f"{label} must not have leading or trailing whitespace")
    if not (_ROLE_RE if role else _ID_RE).fullmatch(value):
        raise ContractError(f"{label} has an invalid identifier")
    return value


def skill_name(value: object, label: str = "skill_name") -> str:
    if type(value) is not str or not _SKILL_RE.fullmatch(value):
        raise ContractError(f"{label} must match [a-z][a-z0-9-]{{0,63}}")
    if value.endswith("-") or "--" in value:
        raise ContractError(f"{label} must not end with a hyphen or contain a double hyphen")
    return value


def version(value: object, label: str = "version") -> str:
    if type(value) is not str or not _VERSION_RE.fullmatch(value):
        raise ContractError(f"{label} must be canonical MAJOR.MINOR.PATCH")
    return value


def digest(value: object, label: str = "digest") -> str:
    if type(value) is not str or not _DIGEST_RE.fullmatch(value):
        raise ContractError(f"{label} must be a lowercase SHA-256 digest")
    return value


def optional_digest(value: object, label: str) -> str | None:
    if value is None:
        return None
    return digest(value, label)


def bounded_items(
    value: object,
    label: str,
    minimum: int,
    maximum: int,
    *,
    sort: bool,
    item_maximum: int = 64,
    role: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ContractError(f"{label} must be an array")
    if not minimum <= len(value) <= maximum:
        raise ContractError(f"{label} must contain {minimum} to {maximum} items")
    items = tuple(identifier(item, f"{label} item", role=role) for item in value)
    if any(len(item) > item_maximum for item in items):
        raise ContractError(f"{label} item exceeds {item_maximum} characters")
    if len(set(items)) != len(items):
        raise ContractError(f"{label} must not contain duplicates")
    if sort and items != tuple(sorted(items)):
        raise ContractError(f"{label} must be sorted")
    return items


def bounded_text_items(value: object, label: str, maximum: int = 32) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not 0 <= len(value) <= maximum:
        raise ContractError(f"{label} must contain 0 to {maximum} items")
    items = tuple(text(item, f"{label} item", 2_048, multiline=True) for item in value)
    if len(set(items)) != len(items):
        raise ContractError(f"{label} must not contain duplicates")
    return items


def ordered_text_items(value: object, label: str, minimum: int, maximum: int) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not minimum <= len(value) <= maximum:
        raise ContractError(f"{label} must contain {minimum} to {maximum} items")
    items = tuple(text(item, f"{label} item", 2_048, multiline=True) for item in value)
    if len(set(items)) != len(items):
        raise ContractError(f"{label} must not contain duplicates")
    return items


def verify_digest(payload: dict[str, object], supplied: object, label: str) -> str:
    expected = digest_json(payload)
    actual = digest(supplied, label)
    if actual != expected:
        raise ContractError(f"{label} does not match canonical content")
    return actual
