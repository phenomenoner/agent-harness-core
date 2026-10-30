"""Strict TOML configuration for the standalone AHC application."""

from .documents import CONFIG_DOCTOR_SCHEMA_VERSION
from .loader import (
    CONFIG_VERSION,
    CONFIG_VERSION_V1,
    ConfigDocument,
    ConfigError,
    ServiceConfig,
    load_config,
    load_config_document,
    parse_config,
)
from .profile_loader import ProfileRegistryLoadError, load_profile_registry

__all__ = [
    "CONFIG_VERSION",
    "CONFIG_VERSION_V1",
    "CONFIG_DOCTOR_SCHEMA_VERSION",
    "ConfigDocument",
    "ConfigError",
    "load_config",
    "load_config_document",
    "load_profile_registry",
    "parse_config",
    "ProfileRegistryLoadError",
    "ServiceConfig",
]
