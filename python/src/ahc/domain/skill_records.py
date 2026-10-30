"""Persisted selection manifests and episode evidence records."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast

from .sessions import ContractError, VirtualSessionIdentity, _identifier
from .skill_guard import guard_payload
from .skill_values import (
    COGNITIVE_LAYERS,
    SKILL_EPISODE_SCHEMA_VERSION,
    SKILL_MANIFEST_SCHEMA_VERSION,
    canonical_json,
    digest,
    digest_json,
    exact_mapping,
    identifier,
    optional_digest,
    schema,
    skill_name,
    text,
)


def _references(value: object, label: str, maximum: int = 32) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) > maximum:
        raise ContractError(f"{label} must contain 0 to {maximum} references")
    items = tuple(text(item, f"{label} item", 512) for item in value)
    if len(set(items)) != len(items) or items != tuple(sorted(items)):
        raise ContractError(f"{label} must be sorted and unique")
    return items


def _digest_items(value: object, label: str, maximum: int = 32) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) > maximum:
        raise ContractError(f"{label} must contain 0 to {maximum} digests")
    items = tuple(digest(item, f"{label} item") for item in value)
    if len(set(items)) != len(items):
        raise ContractError(f"{label} must not contain duplicates")
    return items


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    revision_digest: str
    skill_name: str
    cognitive_layer: str
    role: str
    trigger_evidence: tuple[str, ...]
    score: float

    def __post_init__(self) -> None:
        digest(self.revision_digest, "manifest revision_digest")
        skill_name(self.skill_name)
        if self.cognitive_layer not in COGNITIVE_LAYERS:
            raise ContractError("manifest cognitive_layer is not supported")
        identifier(self.role, "manifest role", role=True)
        evidence = _references(self.trigger_evidence, "trigger_evidence")
        if type(self.score) not in (int, float) or not math.isfinite(float(self.score)):
            raise ContractError("manifest score must be finite")
        object.__setattr__(self, "trigger_evidence", evidence)
        object.__setattr__(self, "score", float(self.score))

    def to_dict(self) -> dict[str, object]:
        return {
            "revision_digest": self.revision_digest,
            "skill_name": self.skill_name,
            "cognitive_layer": self.cognitive_layer,
            "role": self.role,
            "trigger_evidence": list(self.trigger_evidence),
            "score": self.score,
        }

    @classmethod
    def from_dict(cls, value: object) -> ManifestEntry:
        raw = exact_mapping(
            value,
            frozenset(
                {
                    "revision_digest",
                    "skill_name",
                    "cognitive_layer",
                    "role",
                    "trigger_evidence",
                    "score",
                }
            ),
            "manifest entry",
        )
        return cls(
            cast(str, raw["revision_digest"]),
            cast(str, raw["skill_name"]),
            cast(str, raw["cognitive_layer"]),
            cast(str, raw["role"]),
            cast(tuple[str, ...], raw["trigger_evidence"]),
            cast(float, raw["score"]),
        )


@dataclass(frozen=True, slots=True)
class SkillManifest:
    schema_version: str
    session: VirtualSessionIdentity
    turn_id: str
    goal_digest: str | None
    entries: tuple[ManifestEntry, ...]
    manifest_digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, SKILL_MANIFEST_SCHEMA_VERSION, "skill manifest")
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("manifest session must be a VirtualSessionIdentity")
        _identifier(self.turn_id, "manifest turn_id")
        goal = optional_digest(self.goal_digest, "manifest goal_digest")
        entries = tuple(self.entries)
        if len(entries) > 32 or any(type(item) is not ManifestEntry for item in entries):
            raise ContractError("manifest entries must contain at most 32 ManifestEntry values")
        if len({(item.revision_digest, item.role) for item in entries}) != len(entries):
            raise ContractError("manifest entries must be unique per revision and role")
        payload = {
            "schema_version": self.schema_version,
            "session": self.session.to_dict(),
            "turn_id": self.turn_id,
            "goal_digest": goal,
            "entries": [item.to_dict() for item in entries],
        }
        actual = digest_json(payload)
        if self.manifest_digest is not None and self.manifest_digest != actual:
            raise ContractError("manifest_digest does not match canonical manifest")
        object.__setattr__(self, "goal_digest", goal)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "manifest_digest", actual)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "session": self.session.to_dict(),
            "turn_id": self.turn_id,
            "goal_digest": self.goal_digest,
            "entries": [item.to_dict() for item in self.entries],
            "manifest_digest": self.manifest_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> SkillManifest:
        raw = exact_mapping(
            value,
            frozenset(
                {
                    "schema_version",
                    "session",
                    "turn_id",
                    "goal_digest",
                    "entries",
                    "manifest_digest",
                }
            ),
            "skill manifest",
        )
        session = VirtualSessionIdentity.from_dict(cast(dict[str, object], raw["session"]))
        entries = tuple(
            ManifestEntry.from_dict(item) for item in cast(list[object], raw["entries"])
        )
        return cls(
            cast(str, raw["schema_version"]),
            session,
            cast(str, raw["turn_id"]),
            cast(str | None, raw["goal_digest"]),
            entries,
            cast(str | None, raw["manifest_digest"]),
        )


@dataclass(frozen=True, slots=True)
class SkillEpisode:
    schema_version: str
    episode_id: str
    session: VirtualSessionIdentity
    turn_id: str
    task_digest: str
    manifest_digest: str
    surfaced_revisions: tuple[str, ...]
    disclosed_revisions: tuple[str, ...]
    used_revisions: tuple[str, ...]
    outcome: str = "unknown"
    evidence_refs: tuple[str, ...] = ()
    lesson: dict[str, object] | None = None
    attribution_digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, SKILL_EPISODE_SCHEMA_VERSION, "skill episode")
        _identifier(self.episode_id, "episode_id")
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("episode session must be a VirtualSessionIdentity")
        _identifier(self.turn_id, "episode turn_id")
        digest(self.task_digest, "task_digest")
        digest(self.manifest_digest, "manifest_digest")
        if self.outcome not in ("unknown", "verified-success", "verified-failure", "corrected"):
            raise ContractError("episode outcome is unsupported")
        refs = _references(self.evidence_refs, "evidence_refs")
        if self.outcome != "unknown" and not refs:
            raise ContractError("verified outcomes require evidence references")
        surfaced = _digest_items(self.surfaced_revisions, "surfaced_revisions")
        disclosed = _digest_items(self.disclosed_revisions, "disclosed_revisions")
        used = _digest_items(self.used_revisions, "used_revisions")
        if not set(disclosed).issubset(surfaced) or not set(used).issubset(disclosed):
            raise ContractError("episode event sets must be surfaced >= disclosed >= used")
        attribution = optional_digest(self.attribution_digest, "attribution_digest")
        if self.lesson is not None:
            if not isinstance(self.lesson, dict) or len(canonical_json(self.lesson)) > 8_192:
                raise ContractError("lesson must be a bounded JSON object")
            guard_payload(self.lesson)
        object.__setattr__(self, "surfaced_revisions", surfaced)
        object.__setattr__(self, "disclosed_revisions", disclosed)
        object.__setattr__(self, "used_revisions", used)
        object.__setattr__(self, "evidence_refs", refs)
        object.__setattr__(self, "attribution_digest", attribution)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "episode_id": self.episode_id,
            "session": self.session.to_dict(),
            "turn_id": self.turn_id,
            "task_digest": self.task_digest,
            "manifest_digest": self.manifest_digest,
            "surfaced_revisions": list(self.surfaced_revisions),
            "disclosed_revisions": list(self.disclosed_revisions),
            "used_revisions": list(self.used_revisions),
            "outcome": self.outcome,
            "evidence_refs": list(self.evidence_refs),
            "lesson": self.lesson,
            "attribution_digest": self.attribution_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> SkillEpisode:
        keys = frozenset(
            {
                "schema_version",
                "episode_id",
                "session",
                "turn_id",
                "task_digest",
                "manifest_digest",
                "surfaced_revisions",
                "disclosed_revisions",
                "used_revisions",
                "outcome",
                "evidence_refs",
                "lesson",
                "attribution_digest",
            }
        )
        raw = exact_mapping(value, keys, "skill episode")
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["episode_id"]),
            VirtualSessionIdentity.from_dict(cast(dict[str, object], raw["session"])),
            cast(str, raw["turn_id"]),
            cast(str, raw["task_digest"]),
            cast(str, raw["manifest_digest"]),
            cast(tuple[str, ...], raw["surfaced_revisions"]),
            cast(tuple[str, ...], raw["disclosed_revisions"]),
            cast(tuple[str, ...], raw["used_revisions"]),
            cast(str, raw["outcome"]),
            cast(tuple[str, ...], raw["evidence_refs"]),
            cast(dict[str, object] | None, raw["lesson"]),
            cast(str | None, raw["attribution_digest"]),
        )


__all__ = ["ManifestEntry", "SkillEpisode", "SkillManifest"]
