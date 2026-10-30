"""Immutable standalone configuration documents and doctor projection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ahc.domain import AgentProfileRegistry

CONFIG_DOCTOR_SCHEMA_VERSION = "ahc.config-doctor.v1"


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    """Validated service data before conversion to the domain API."""

    name: str
    command: tuple[str, ...]
    cwd: Path
    enabled: bool
    restart_policy: str
    max_restarts: int
    restart_delay_seconds: float
    stop_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class ConfigDocument:
    """A fully validated configuration document with resolved paths."""

    config_version: str
    state_dir: Path
    services: tuple[ServiceConfig, ...]
    source_path: Path
    profile_registry: AgentProfileRegistry | None = None
    profile_registry_path: Path | None = None
    foreground_configs: tuple[tuple[str, Path], ...] = ()

    def summary(self) -> dict[str, Any]:
        """Return the versioned secret-safe doctor projection."""
        return {
            "schema_version": CONFIG_DOCTOR_SCHEMA_VERSION,
            "config_version": self.config_version,
            "service_count": len(self.services),
            "enabled_service_count": sum(service.enabled for service in self.services),
            "services": [
                {
                    "name": service.name,
                    "enabled": service.enabled,
                    "restart_policy": service.restart_policy,
                }
                for service in self.services
            ],
        }

    def to_domain_config(self) -> Any:
        """Construct the Worker A ``ahc.domain.AppConfig`` value."""
        from .loader import _to_domain_config

        return _to_domain_config(self)
