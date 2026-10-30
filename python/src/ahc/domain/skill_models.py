"""Immutable skill assets and deterministic selection request values."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import cast

from .sessions import ContractError, VirtualSessionIdentity, _identifier
from .skill_assets import MARKDOWN_SKILL_SCHEMA, MAX_BUNDLE_BYTES, SkillAsset, markdown_body
from .skill_guard import guard_payload, guard_raw
from .skill_values import (
    COGNITIVE_LAYERS,
    SKILL_RELATION_KINDS,
    SKILL_REVISION_SCHEMA_VERSION,
    bounded_items,
    bounded_text_items,
    digest_bytes,
    digest_json,
    exact_mapping,
    identifier,
    normalize_body,
    optional_digest,
    ordered_text_items,
    schema,
    skill_name,
    text,
    version,
)

_HEADING_ORDER = ("## Purpose", "## Activation", "## Procedure", "## Verification", "## Boundaries")
_RELATION_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _triggers(value: object) -> tuple[str, ...]:
    items = ordered_text_items(value, "triggers", 1, 32)
    if any(len(item) > 128 for item in items):
        raise ContractError("triggers item exceeds 128 characters")
    if items != tuple(sorted(items)):
        raise ContractError("triggers must be sorted")
    return items


def _body_sections(body: str, name: str) -> None:
    lines = body.rstrip("\n").split("\n")
    if any(line.startswith("```") for line in lines):
        raise ContractError("body must not contain fenced code blocks")
    positions = [index for index, line in enumerate(lines) if line in _HEADING_ORDER]
    if tuple(lines[index] for index in positions) != _HEADING_ORDER or len(positions) != 5:
        raise ContractError("body headings must be the required ordered level-two sections")
    prefix = lines[: positions[0]]
    if prefix and (prefix[0] != f"# {name}" or any(line != "" for line in prefix[1:])):
        raise ContractError("only one skill title may precede the first heading")


@dataclass(frozen=True, slots=True)
class SkillRelationship:
    kind: str
    target: str

    def __post_init__(self) -> None:
        if self.kind not in SKILL_RELATION_KINDS:
            raise ContractError(f"relationship kind {self.kind!r} is unsupported")
        if type(self.target) is not str or not _RELATION_PATTERN.fullmatch(self.target):
            raise ContractError("relationship target must be an exact revision digest")

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "target": self.target}

    @classmethod
    def from_dict(cls, value: object) -> SkillRelationship:
        raw = exact_mapping(value, frozenset({"kind", "target"}), "relationship")
        return cls(cast(str, raw["kind"]), cast(str, raw["target"]))


@dataclass(frozen=True, slots=True)
class SkillRevision:
    schema_version: str
    skill_name: str
    version: str
    description: str
    cognitive_layer: str
    roles: tuple[str, ...]
    agent_allowlist: tuple[str, ...]
    required_tools: tuple[str, ...]
    triggers: tuple[str, ...]
    expected_effects: tuple[str, ...]
    positive_examples: tuple[str, ...]
    negative_examples: tuple[str, ...]
    relationships: tuple[SkillRelationship, ...]
    predecessor_digest: str | None
    body: str
    body_digest: str | None = None
    revision_digest: str | None = None
    assets: tuple[SkillAsset, ...] = ()

    def __post_init__(self) -> None:
        modern = self.schema_version == MARKDOWN_SKILL_SCHEMA
        if not modern:
            schema(self.schema_version, SKILL_REVISION_SCHEMA_VERSION, "skill revision")
        assets = tuple(self.assets)
        if (len(assets) > 256 or any(type(item) is not SkillAsset for item in assets)
                or (assets and not modern)):
            raise ContractError("resources require a v2 revision and at most 256 assets")
        asset_paths = {item.path.casefold() for item in assets}
        if (len(asset_paths) != len(assets)
                or sum(item.size for item in assets) > MAX_BUNDLE_BYTES):
            raise ContractError("skill resources collide or exceed bundle bound")
        if any("/".join(path.split("/")[:index]) in asset_paths
               for path in asset_paths for index in range(1, len(path.split("/")))):
            raise ContractError("skill resource file and directory paths collide")
        object.__setattr__(self, "assets", tuple(sorted(assets, key=lambda item: item.path)))
        skill_name(self.skill_name)
        version(self.version)
        text(self.description, "description", 512, multiline=True)
        if self.cognitive_layer not in COGNITIVE_LAYERS:
            raise ContractError("cognitive_layer is not supported")
        roles = bounded_items(self.roles, "roles", 1, 16, sort=True, role=True)
        agents = bounded_items(self.agent_allowlist, "agent_allowlist", 0, 32, sort=True)
        tools = bounded_items(self.required_tools, "required_tools", 0, 32, sort=True, role=True)
        triggers = _triggers(self.triggers)
        effects = bounded_text_items(self.expected_effects, "expected_effects")
        positive = ordered_text_items(self.positive_examples, "positive_examples", 1, 16)
        negative = ordered_text_items(self.negative_examples, "negative_examples", 1, 16)
        relations = tuple(self.relationships)
        if len(relations) > 64 or any(type(item) is not SkillRelationship for item in relations):
            raise ContractError("relationships must contain 0 to 64 SkillRelationship values")
        if len({(item.kind, item.target) for item in relations}) != len(relations):
            raise ContractError("relationships must not contain duplicates")
        relations = tuple(sorted(relations, key=lambda item: (item.kind, item.target)))
        predecessor = optional_digest(self.predecessor_digest, "predecessor_digest")
        body = markdown_body(self.body) if modern else normalize_body(self.body)
        if not modern:
            _body_sections(body, self.skill_name)
        body_digest = digest_bytes(body.encode("utf-8"))
        payload = self._payload(body=body, relations=relations, body_digest=body_digest)
        if not modern:
            guard_payload(payload)
            guard_raw(body.encode("utf-8"))
        actual_revision_digest = digest_json(payload)
        if self.body_digest is not None and self.body_digest != body_digest:
            raise ContractError("body_digest does not match canonical body")
        if self.revision_digest is not None and self.revision_digest != actual_revision_digest:
            raise ContractError("revision_digest does not match canonical metadata and body")
        for name, value in (
            ("roles", roles),
            ("agent_allowlist", agents),
            ("required_tools", tools),
            ("triggers", triggers),
            ("expected_effects", effects),
            ("positive_examples", positive),
            ("negative_examples", negative),
            ("relationships", relations),
            ("predecessor_digest", predecessor),
            ("body", body),
            ("body_digest", body_digest),
            ("revision_digest", actual_revision_digest),
        ):
            object.__setattr__(self, name, value)

    def _payload(
        self, *, body: str, relations: tuple[SkillRelationship, ...], body_digest: str
    ) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "skill_name": self.skill_name,
            "version": self.version,
            "description": self.description,
            "cognitive_layer": self.cognitive_layer,
            "roles": list(self.roles),
            "agent_allowlist": list(self.agent_allowlist),
            "required_tools": list(self.required_tools),
            "triggers": list(self.triggers),
            "expected_effects": list(self.expected_effects),
            "positive_examples": list(self.positive_examples),
            "negative_examples": list(self.negative_examples),
            "relationships": [item.to_dict() for item in relations],
            "predecessor_digest": self.predecessor_digest,
            "body": body,
            "body_digest": body_digest,
            **({"assets": [item.to_dict() for item in self.assets]}
               if self.schema_version == MARKDOWN_SKILL_SCHEMA else {}),
        }

    def to_dict(self) -> dict[str, object]:
        payload = self._payload(
            body=self.body, relations=self.relationships, body_digest=self.body_digest or ""
        )
        payload["revision_digest"] = self.revision_digest
        return payload

    @classmethod
    def from_dict(cls, value: object) -> SkillRevision:
        keys = frozenset(
            {
                "schema_version",
                "skill_name",
                "version",
                "description",
                "cognitive_layer",
                "roles",
                "agent_allowlist",
                "required_tools",
                "triggers",
                "expected_effects",
                "positive_examples",
                "negative_examples",
                "relationships",
                "predecessor_digest",
                "body",
                "body_digest",
                "revision_digest",
            }
        )
        if isinstance(value, dict) and value.get("schema_version") == MARKDOWN_SKILL_SCHEMA:
            keys = keys | {"assets"}
        raw = exact_mapping(value, keys, "skill revision")
        relations = tuple(
            SkillRelationship.from_dict(item) for item in cast(list[object], raw["relationships"])
        )
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["skill_name"]),
            cast(str, raw["version"]),
            cast(str, raw["description"]),
            cast(str, raw["cognitive_layer"]),
            cast(tuple[str, ...], raw["roles"]),
            cast(tuple[str, ...], raw["agent_allowlist"]),
            cast(tuple[str, ...], raw["required_tools"]),
            cast(tuple[str, ...], raw["triggers"]),
            cast(tuple[str, ...], raw["expected_effects"]),
            cast(tuple[str, ...], raw["positive_examples"]),
            cast(tuple[str, ...], raw["negative_examples"]),
            relations,
            cast(str | None, raw["predecessor_digest"]),
            cast(str, raw["body"]),
            cast(str | None, raw["body_digest"]),
            cast(str | None, raw["revision_digest"]),
            tuple(SkillAsset.from_dict(item) for item in raw.get("assets", ())),
        )


@dataclass(frozen=True, slots=True)
class SelectionRequest:
    session: VirtualSessionIdentity
    turn_id: str
    roles: tuple[str, ...]
    agent_id: str
    available_tools: tuple[str, ...]
    task_text: str
    goal_digest: str | None = None

    def __post_init__(self) -> None:
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("selection session must be a VirtualSessionIdentity")
        _identifier(self.turn_id, "turn_id")
        roles = bounded_items(self.roles, "requested roles", 1, 16, sort=True, role=True)
        identifier(self.agent_id, "agent_id")
        if self.agent_id != self.session.agent_id:
            raise ContractError("selection request agent must match the session agent")
        tools = bounded_items(self.available_tools, "available tools", 0, 64, sort=True, role=True)
        task = text(self.task_text, "task_text", 16_384, multiline=True)
        object.__setattr__(self, "roles", roles)
        object.__setattr__(self, "available_tools", tools)
        object.__setattr__(self, "task_text", task)
        object.__setattr__(self, "goal_digest", optional_digest(self.goal_digest, "goal_digest"))


__all__ = [
    "COGNITIVE_LAYERS",
    "SelectionRequest",
    "SkillRelationship",
    "SkillRevision",
    "SKILL_REVISION_SCHEMA_VERSION",
]
