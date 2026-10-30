"""Convert validated configuration into the M0 domain model."""

from __future__ import annotations

from ahc.domain import AppConfig, RestartPolicy, ServiceSpec

from .loader import ConfigDocument


def to_domain_config(document: ConfigDocument) -> AppConfig:
    """Construct the exact domain values consumed by the lifecycle application."""

    services = tuple(
        ServiceSpec(
            name=service.name,
            command=service.command,
            cwd=service.cwd,
            restart_policy=RestartPolicy(service.restart_policy),
            max_restarts=service.max_restarts,
            restart_delay_seconds=service.restart_delay_seconds,
            stop_timeout_seconds=service.stop_timeout_seconds,
        )
        for service in document.services
        if service.enabled
    )
    return AppConfig(
        state_dir=document.state_dir,
        services=services,
        profile_registry=document.profile_registry,
        foreground_configs=document.foreground_configs,
    )
