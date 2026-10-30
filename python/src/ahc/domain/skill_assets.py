"""Versioned skill resources: inert, hash-bound data, never effect authority."""

from dataclasses import dataclass

from .sessions import ContractError
from .skill_values import digest, exact_mapping

MARKDOWN_SKILL_SCHEMA = "ahc.skill-revision.v2"
MAX_ASSET_BYTES = 16 * 1024 * 1024
MAX_BUNDLE_BYTES = 64 * 1024 * 1024


def asset_path(value: object) -> str:
    if type(value) is not str or not 1 <= len(value) <= 240:
        raise ContractError("skill resource path is invalid")
    parts = value.split("/")
    if any(
        not part
        or part in (".", "..")
        or part.rstrip(" .") != part
        or any(ord(c) < 32 or c in '\\:*?"<>|' for c in part)
        or part.split(".")[0].upper()
        in {
            "CON",
            "PRN",
            "AUX",
            "NUL",
            *(f"{kind}{n}" for kind in ("COM", "LPT") for n in range(1, 10)),
        }
        for part in parts
    ) or parts[0].casefold() in {"skill.md", "manifest.json", ".git"}:
        raise ContractError("skill resource must have a portable relative path")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ContractError("skill resource path is not UTF-8") from exc
    return value


@dataclass(frozen=True, slots=True)
class SkillAsset:
    path: str
    sha256: str
    size: int

    def __post_init__(self):
        asset_path(self.path)
        digest(self.sha256, "skill resource digest")
        if type(self.size) is not int or not 0 <= self.size <= MAX_ASSET_BYTES:
            raise ContractError("skill resource size exceeds bound")

    def to_dict(self):
        return {"path": self.path, "sha256": self.sha256, "size": self.size}

    @classmethod
    def from_dict(cls, value):
        raw = exact_mapping(value, frozenset({"path", "sha256", "size"}), "skill resource")
        return cls(**raw)


def markdown_body(value: object) -> str:
    # Preserve original Unicode, newlines and Markdown, including fenced examples.
    if type(value) is not str or not value.strip() or "\x00" in value:
        raise ContractError("skill Markdown must be nonempty text")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as exc:
        raise ContractError("skill Markdown is not UTF-8") from exc
    if len(encoded) > 262144:
        raise ContractError("skill Markdown exceeds 256 KiB")
    return value
