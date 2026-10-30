"""The single explicit, conservative content guard for ASI assets."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping, Sequence

_ACTION_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "legacy-control": (
        re.compile(
            r"\b(?:run|execute|invoke|launch|start|schedule)\b.{0,120}\b(?:task scheduler|"
            r"schtasks|win32com|comtypes|subprocess|os\.system|powershell|cmd\.exe|process "
            r"control|working scheduler)\b"
        ),
        re.compile(r"\b(?:read|write|modify|execute)\b.{0,120}\.agent-harness\b"),
    ),
    "external-effect": (
        re.compile(
            r"\b(?:send|post|upload|exfiltrate)\b(?:\s+\w+){0,10}\s+(?:to|via)\s+(?:telegram|discord|provider|channel|network|https?://)"
        ),
        re.compile(r"\b(?:connect|fetch|call)\b(?:\s+\w+){0,10}\s+(?:to|via)?\s*https?://"),
        re.compile(r"\btransmit\b(?:\s+\w+){0,10}\s+(?:to|via|over)\s+(?:network|socket|https?://)"),
        re.compile(r"\b(?:deploy|cutover|install service)\b"),
    ),
    "authority-mutation": (
        re.compile(
            r"\b(?:rewrite policy|change permission|grant permission|bypass policy|write memory|"
            r"memory store)\b"
        ),
    ),
    "credential": (
        re.compile(r"\b(?:api[ _-]?key|password)\s*[:=]\s*\S+"),
        re.compile(r"\bauthorization\s*:\s*bearer\s+\S+"),
        re.compile(r"\bprivate-key\s*[:=]\s*\S+"),
    ),
}

_PATH_PATTERNS = (
    re.compile(r"(?<![a-z0-9])[a-z]:/", re.IGNORECASE),
    re.compile(r"(?<![a-z0-9:])//[^/\s]+/[^\s]+"),
    re.compile(r"(?<![a-z0-9])/mnt/[a-z]/", re.IGNORECASE),
    re.compile(r"(?<![a-z0-9])/home/", re.IGNORECASE),
    re.compile(r"(?<![a-z0-9])~/\.", re.IGNORECASE),
)


class GuardViolation(ValueError):
    """A bounded non-semantic guard found a protected family."""

    def __init__(self, family: str) -> None:
        self.family = family
        super().__init__(f"content guard blocked family {family!r}")


def normalize_guard_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold().replace("\\", "/")
    return re.sub(r"\s+", " ", normalized).strip()


def guard_text(value: object) -> None:
    if type(value) is not str:
        return
    normalized = normalize_guard_text(value)
    for family, patterns in _ACTION_PATTERNS.items():
        if any(pattern.search(normalized) for pattern in patterns):
            raise GuardViolation(family)
    if any(pattern.search(normalized) for pattern in _PATH_PATTERNS):
        raise GuardViolation("machine-private-path")


def guard_payload(value: object) -> None:
    if isinstance(value, str):
        guard_text(value)
    elif isinstance(value, Mapping):
        for key, item in value.items():
            guard_text(key)
            guard_payload(item)
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        for item in value:
            guard_payload(item)


def guard_raw(value: bytes) -> None:
    if type(value) is not bytes:
        raise GuardViolation("invalid-utf8")
    try:
        decoded = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise GuardViolation("invalid-utf8") from exc
    guard_text(decoded)
