"""Parse and validate the versioned standalone TOML configuration."""

from __future__ import annotations

import math
import tomllib
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from ahc.domain import AgentProfileRegistry

from .documents import ConfigDocument, ServiceConfig
from .profile_loader import ProfileRegistryLoadError, load_profile_registry

CONFIG_VERSION_V1 = "ahc.standalone.config.v1"
CONFIG_VERSION = "ahc.standalone.config.v2"
RESTART_POLICIES = ("never", "on-failure", "always")

MAX_SERVICE_NAME_LENGTH = 128
MAX_COMMAND_ARGUMENTS = 128
MAX_RESTARTS = 100
MAX_DELAY_SECONDS = 3_600.0
MIN_STOP_TIMEOUT_SECONDS = 0.1
MAX_STOP_TIMEOUT_SECONDS = 3_600.0

_V1_TOP_LEVEL_FIELDS = frozenset({"config_version", "state_dir", "services"})
_V2_TOP_LEVEL_FIELDS = frozenset({"config_version", "state_dir", "services", "profile_registry"})
_SERVICE_FIELDS = frozenset(
    {
        "name",
        "command",
        "cwd",
        "enabled",
        "restart_policy",
        "max_restarts",
        "restart_delay_seconds",
        "stop_timeout_seconds",
    }
)
_DEFAULTS: dict[str, Any] = {
    "enabled": True,
    "restart_policy": "never",
    "max_restarts": 0,
    "restart_delay_seconds": 0.0,
    "stop_timeout_seconds": 10.0,
}


class ConfigError(ValueError):
    """A configuration file is unreadable or violates the M0 contract."""


def load_config_document(path: str | Path) -> ConfigDocument:
    """Load and strictly validate TOML without requiring Worker A's domain module."""
    return _load_config_document(path, validate_service_paths=True)


def _load_config_document_for_doctor(path: str | Path) -> ConfigDocument:
    return _load_config_document(path, validate_service_paths=False)


def _load_config_document(path: str | Path, *, validate_service_paths: bool) -> ConfigDocument:
    source_path = Path(path).expanduser().resolve(strict=False)
    try:
        with source_path.open("rb") as config_file:
            raw = tomllib.load(config_file)
    except OSError as exc:
        raise ConfigError("cannot read configured AHC TOML") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError("invalid TOML in configured AHC document") from exc
    return _parse_document(raw, source_path, validate_service_paths=validate_service_paths)


parse_config = load_config_document


def load_config(path: str | Path) -> Any:
    """Load TOML and return the domain ``AppConfig`` expected by lifecycle code."""
    return load_config_document(path).to_domain_config()


def _parse_document(raw: Any, source_path: Path, *, validate_service_paths: bool) -> ConfigDocument:
    _require_mapping(raw, "root")
    _require_keys(raw, {"config_version"}, "root")
    version = _required_string(raw["config_version"], "root.config_version")
    if version == CONFIG_VERSION_V1:
        fields = _V1_TOP_LEVEL_FIELDS
    elif version == CONFIG_VERSION:
        fields = _V2_TOP_LEVEL_FIELDS
    else:
        raise ConfigError(
            f"root.config_version must be {CONFIG_VERSION_V1!r} or {CONFIG_VERSION!r}"
        )
    allowed = fields | {"foreground_configs"} if version == CONFIG_VERSION else fields
    _reject_unknown(raw, allowed, "root")
    _require_keys(raw, fields, "root")

    state_dir = _resolve_path(
        raw["state_dir"],
        source_path.parent,
        "root.state_dir",
        canonicalize=validate_service_paths,
    )
    raw_services = raw["services"]
    if not isinstance(raw_services, list):
        raise ConfigError("root.services must be an array of tables")
    services: list[ServiceConfig] = []
    names: set[str] = set()
    for index, raw_service in enumerate(raw_services):
        context = f"root.services[{index}]"
        service = _parse_service(
            raw_service,
            source_path.parent,
            context,
            validate_cwd=validate_service_paths,
        )
        if service.name in names:
            raise ConfigError(f"{context}.name duplicates service {service.name!r}")
        names.add(service.name)
        services.append(service)

    registry_path: Path | None = None
    registry: AgentProfileRegistry | None = None
    if version == CONFIG_VERSION:
        registry_path = _resolve_relative_profile_registry_path(
            raw["profile_registry"], source_path.parent, "root.profile_registry"
        )
        try:
            registry = load_profile_registry(registry_path)
        except ProfileRegistryLoadError as exc:
            raise ConfigError(str(exc)) from exc
    from .foreground_paths import foreground_paths

    foregrounds = foreground_paths(raw.get("foreground_configs"), registry, source_path.parent)
    return ConfigDocument(
        version,
        state_dir,
        tuple(services),
        source_path,
        registry,
        registry_path,
        foregrounds,
    )


def _parse_service(raw: Any, base_dir: Path, context: str, *, validate_cwd: bool) -> ServiceConfig:
    _require_mapping(raw, context)
    _reject_unknown(raw, _SERVICE_FIELDS, context)
    _require_keys(raw, {"name", "command", "cwd"}, context)

    name = _required_string(raw["name"], f"{context}.name")
    if len(name) > MAX_SERVICE_NAME_LENGTH:
        raise ConfigError(f"{context}.name must be at most {MAX_SERVICE_NAME_LENGTH} characters")

    command = raw["command"]
    if not isinstance(command, list) or not command:
        raise ConfigError(f"{context}.command must be a non-empty array of strings")
    if len(command) > MAX_COMMAND_ARGUMENTS:
        raise ConfigError(
            f"{context}.command must contain at most {MAX_COMMAND_ARGUMENTS} arguments"
        )
    if any(type(argument) is not str or not argument for argument in command):
        raise ConfigError(f"{context}.command must contain only non-empty strings")

    cwd = _resolve_path(raw["cwd"], base_dir, f"{context}.cwd", canonicalize=validate_cwd)
    if validate_cwd and (not cwd.exists() or not cwd.is_dir()):
        raise ConfigError(f"{context}.cwd does not exist as a directory")

    enabled = raw.get("enabled", _DEFAULTS["enabled"])
    if type(enabled) is not bool:
        raise ConfigError(f"{context}.enabled must be a boolean")

    restart_policy = raw.get("restart_policy", _DEFAULTS["restart_policy"])
    restart_policy = _required_string(restart_policy, f"{context}.restart_policy")
    if restart_policy not in RESTART_POLICIES:
        choices = ", ".join(repr(policy) for policy in RESTART_POLICIES)
        raise ConfigError(f"{context}.restart_policy must be one of {choices}")

    max_restarts = raw.get("max_restarts", _DEFAULTS["max_restarts"])
    if type(max_restarts) is not int or not 0 <= max_restarts <= MAX_RESTARTS:
        raise ConfigError(f"{context}.max_restarts must be an integer in [0, {MAX_RESTARTS}]")

    restart_delay = _bounded_float(
        raw.get("restart_delay_seconds", _DEFAULTS["restart_delay_seconds"]),
        f"{context}.restart_delay_seconds",
        0.0,
        MAX_DELAY_SECONDS,
    )
    stop_timeout = _bounded_float(
        raw.get("stop_timeout_seconds", _DEFAULTS["stop_timeout_seconds"]),
        f"{context}.stop_timeout_seconds",
        MIN_STOP_TIMEOUT_SECONDS,
        MAX_STOP_TIMEOUT_SECONDS,
    )

    return ServiceConfig(
        name=name,
        command=tuple(command),
        cwd=cwd,
        enabled=enabled,
        restart_policy=restart_policy,
        max_restarts=max_restarts,
        restart_delay_seconds=restart_delay,
        stop_timeout_seconds=stop_timeout,
    )


def _resolve_path(value: Any, base_dir: Path, context: str, *, canonicalize: bool = True) -> Path:
    path_value = _required_string(value, context)
    path = base_dir / path_value
    return path.resolve(strict=False) if canonicalize else path.absolute()


def _resolve_relative_profile_registry_path(value: Any, base_dir: Path, context: str) -> Path:
    path_value = _required_string(value, context)
    if PurePosixPath(path_value).anchor or PureWindowsPath(path_value).anchor:
        raise ConfigError(f"{context} must be a relative path")
    return (base_dir / path_value).resolve(strict=False)


def _required_string(value: Any, context: str) -> str:
    if type(value) is not str or not value.strip():
        raise ConfigError(f"{context} must be a non-empty string")
    return value


def _bounded_float(value: Any, context: str, lower: float, upper: float) -> float:
    if type(value) not in (int, float):
        raise ConfigError(f"{context} must be a finite number in [{lower}, {upper}]")
    numeric = float(value)
    if not math.isfinite(numeric) or not lower <= numeric <= upper:
        raise ConfigError(f"{context} must be a finite number in [{lower}, {upper}]")
    return numeric


def _require_mapping(value: Any, context: str) -> None:
    if not isinstance(value, dict):
        raise ConfigError(f"{context} must be a TOML table")


def _require_keys(value: dict[str, Any], required: set[str] | frozenset[str], context: str) -> None:
    missing = sorted(required - value.keys())
    if missing:
        raise ConfigError(f"{context} is missing required field(s): {', '.join(missing)}")


def _reject_unknown(value: dict[str, Any], allowed: frozenset[str], context: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ConfigError(f"{context} contains unknown field(s): {', '.join(unknown)}")


def _to_domain_config(document: ConfigDocument) -> Any:
    from .domain import to_domain_config

    return to_domain_config(document)
