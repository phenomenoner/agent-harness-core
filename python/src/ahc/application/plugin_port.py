"""First-party native adapter port values; no lifecycle or mutable state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ahc.domain import ContractError, NativePluginManifest, PluginBinding
from ahc.domain.skill_values import identifier


@dataclass(frozen=True, slots=True)
class AdapterOpenResult:
    """Sanitized adapter-owned readiness returned to the host."""

    status: str
    reason_code: str

    def __post_init__(self) -> None:
        if self.status not in {"ready", "degraded", "unavailable"}:
            raise ContractError("adapter open status is unsupported")
        identifier(self.reason_code, "adapter reason_code")


class AdapterCallError(Exception):
    """Bounded adapter failure classification without provider exception text."""

    def __init__(self, reason_code: str, *, effect_may_have_occurred: bool) -> None:
        identifier(reason_code, "adapter reason_code")
        if type(effect_may_have_occurred) is not bool:
            raise ContractError("effect_may_have_occurred must be a strict boolean")
        super().__init__("native plugin adapter call failed")
        self.reason_code = reason_code
        self.effect_may_have_occurred = effect_may_have_occurred


class NativePluginAdapter(Protocol):
    def open(self) -> AdapterOpenResult: ...

    def invoke(self, capability_id: str, payload: dict[str, Any]) -> dict[str, Any]: ...

    def close(self) -> None: ...


class NativePluginFactory(Protocol):
    def __call__(
        self, manifest: NativePluginManifest, binding: PluginBinding
    ) -> NativePluginAdapter: ...


__all__: list[str] = []
