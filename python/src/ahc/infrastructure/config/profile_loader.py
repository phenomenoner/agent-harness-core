"""Load exactly one explicit M2.1 JSON profile registry."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from ahc.domain import AgentProfileRegistry, ContractError


class ProfileRegistryLoadError(ValueError):
    """The explicit registry file could not become current authority."""


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ProfileRegistryLoadError(
                "configured profile registry contains a duplicate JSON object member"
            )
        result[key] = value
    return result


def load_profile_registry(path: Path) -> AgentProfileRegistry:
    """Read and validate one exact file without discovery or fallback."""

    try:
        text = path.read_bytes().decode("utf-8")
    except OSError as exc:
        raise ProfileRegistryLoadError("cannot read configured profile registry") from exc
    except UnicodeError as exc:
        raise ProfileRegistryLoadError(
            "configured profile registry is not valid UTF-8 JSON"
        ) from exc
    try:
        raw = json.loads(text, object_pairs_hook=_strict_object)
    except ProfileRegistryLoadError:
        raise
    except json.JSONDecodeError as exc:
        raise ProfileRegistryLoadError(
            f"invalid configured profile registry JSON at line {exc.lineno} column {exc.colno}"
        ) from exc
    if not isinstance(raw, Mapping):
        raise ProfileRegistryLoadError("profile registry root must be a JSON object")
    try:
        return AgentProfileRegistry.from_dict(cast(Mapping[str, object], raw))
    except ContractError as exc:
        raise ProfileRegistryLoadError(f"invalid profile registry: {exc}") from exc


__all__ = ["ProfileRegistryLoadError", "load_profile_registry"]
