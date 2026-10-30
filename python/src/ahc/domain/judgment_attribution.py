"""Strict layer-correct outcome-attribution records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from .sessions import ContractError, _identifier
from .skill_values import (
    COGNITIVE_LAYERS,
    digest,
    digest_json,
    exact_mapping,
    identifier,
    ordered_text_items,
    schema,
)


def _finish(payload: dict[str, object], supplied: object, label: str) -> str:
    actual = digest_json(payload)
    if supplied is not None and digest(supplied, label) != actual:
        raise ContractError(f"{label} does not match canonical content")
    return actual


ATTRIBUTION_SCHEMA = "ahc.outcome-attribution.v1"
ATTRIBUTIONS = (
    "wrong-why",
    "wrong-what",
    "wrong-how",
    "execution-slip",
    "environment-drift",
    "insufficient-evidence",
    "mixed",
    "unknown",
)


def _refs(value: object, label: str) -> tuple[str, ...]:
    return ordered_text_items(value, label, 0, 32)


def _layers(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) > 3:
        raise ContractError("affected_layers must contain 0 to 3 layers")
    layers = tuple(value)
    if any(type(layer) is not str or layer not in COGNITIVE_LAYERS for layer in layers):
        raise ContractError("affected_layers contains an unsupported layer")
    if len(set(layers)) != len(layers) or layers != tuple(
        layer for layer in COGNITIVE_LAYERS if layer in layers
    ):
        raise ContractError("affected_layers must be unique in WHY/WHAT/HOW order")
    return layers


@dataclass(frozen=True, slots=True)
class OutcomeAttribution:
    schema_version: str
    episode_id: str
    primary_label: str
    affected_layers: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    verifier_id: str
    digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, ATTRIBUTION_SCHEMA, "outcome attribution")
        _identifier(self.episode_id, "attribution episode_id")
        if self.primary_label not in ATTRIBUTIONS:
            raise ContractError("attribution label is unsupported")
        layers = _layers(self.affected_layers)
        refs = _refs(self.evidence_refs, "attribution evidence_refs")
        identifier(self.verifier_id, "verifier_id")
        required = {
            "wrong-why": ("why-judgment",),
            "wrong-what": ("what-strategy",),
            "wrong-how": ("how-procedure",),
            "execution-slip": ("how-procedure",),
            "environment-drift": ("how-procedure",),
            "insufficient-evidence": (),
            "unknown": (),
            "mixed": layers,
        }
        if (
            self.primary_label in required
            and self.primary_label != "mixed"
            and layers != required[self.primary_label]
        ):
            raise ContractError("attribution layer does not match primary label")
        if self.primary_label == "mixed" and len(layers) < 2:
            raise ContractError("mixed attribution requires at least two layers")
        payload = {
            "schema_version": self.schema_version,
            "episode_id": self.episode_id,
            "primary_label": self.primary_label,
            "affected_layers": list(layers),
            "evidence_refs": list(refs),
            "verifier_id": self.verifier_id,
        }
        actual = _finish(payload, self.digest, "attribution digest")
        object.__setattr__(self, "affected_layers", layers)
        object.__setattr__(self, "evidence_refs", refs)
        object.__setattr__(self, "digest", actual)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "episode_id": self.episode_id,
            "primary_label": self.primary_label,
            "affected_layers": list(self.affected_layers),
            "evidence_refs": list(self.evidence_refs),
            "verifier_id": self.verifier_id,
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> OutcomeAttribution:
        raw = exact_mapping(
            value,
            frozenset(
                {
                    "schema_version",
                    "episode_id",
                    "primary_label",
                    "affected_layers",
                    "evidence_refs",
                    "verifier_id",
                    "digest",
                }
            ),
            "outcome attribution",
        )
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["episode_id"]),
            cast(str, raw["primary_label"]),
            cast(tuple[str, ...], raw["affected_layers"]),
            cast(tuple[str, ...], raw["evidence_refs"]),
            cast(str, raw["verifier_id"]),
            cast(str, raw["digest"]),
        )
