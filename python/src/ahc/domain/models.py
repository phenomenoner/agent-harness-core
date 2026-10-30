"""Pure M0 domain contracts."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import cast
from uuid import UUID

from .profile_registry import AgentProfileRegistry


class RestartPolicy(StrEnum):
    """Policy used after a configured child exits."""

    NEVER = "never"
    ON_FAILURE = "on-failure"
    ALWAYS = "always"

    @classmethod
    def parse(cls, value: RestartPolicy | str) -> RestartPolicy:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as exc:
            values = ", ".join(policy.value for policy in cls)
            raise ValueError(f"unknown restart policy {value!r}; expected {values}") from exc


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    """PID generation: legacy creation time or explicit native Linux generation."""

    pid: int
    start_time: float
    linux_boot_id: str | None = None
    linux_start_ticks: int | None = None
    linux_pid_namespace: int | None = None

    def __post_init__(self) -> None:
        if type(self.pid) is not int or self.pid <= 0:
            raise ValueError("pid must be a positive integer")
        if (
            type(self.start_time) not in (int, float)
            or not math.isfinite(float(self.start_time))
            or self.start_time < 0
        ):
            raise ValueError("start_time must be a finite non-negative number")
        object.__setattr__(self, "start_time", float(self.start_time))
        native = (self.linux_boot_id, self.linux_start_ticks, self.linux_pid_namespace)
        if any(value is not None for value in native):
            try:
                valid_boot = str(UUID(self.linux_boot_id)) == self.linux_boot_id
            except (ValueError, TypeError, AttributeError):
                valid_boot = False
            if (not valid_boot or type(self.linux_start_ticks) is not int
                    or self.linux_start_ticks < 0 or type(self.linux_pid_namespace) is not int
                    or self.linux_pid_namespace <= 0):
                raise ValueError("invalid native process identity")

    def to_dict(self) -> dict[str, object]:
        value = {"pid": self.pid, "start_time": self.start_time}
        if self.linux_boot_id is not None:
            value.update(linux_boot_id=self.linux_boot_id, linux_start_ticks=self.linux_start_ticks,
                         linux_pid_namespace=self.linux_pid_namespace)
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ProcessIdentity:
        try:
            pid = value["pid"]
            start_time = value["start_time"]
        except (KeyError, TypeError) as exc:
            raise ValueError("invalid process identity") from exc
        if type(pid) is not int or type(start_time) not in (int, float):
            raise ValueError("invalid process identity")
        strict_pid = cast(int, pid)
        strict_start_time = cast(int | float, start_time)
        native_keys = {"linux_boot_id", "linux_start_ticks", "linux_pid_namespace"}
        if set(value) & native_keys and not native_keys <= set(value):
            raise ValueError("incomplete native process identity")
        if native_keys <= set(value) and any(value[key] is None for key in native_keys):
            raise ValueError("invalid native process identity")
        return cls(pid=strict_pid, start_time=float(strict_start_time),
                   **{key: value[key] for key in native_keys if key in value})


@dataclass(frozen=True, slots=True)
class ServiceSpec:
    """A directly spawned foreground child and its bounded restart policy."""

    name: str
    command: tuple[str, ...]
    restart_policy: RestartPolicy = RestartPolicy.NEVER
    max_restarts: int = 0
    restart_delay_seconds: float = 0.0
    env: Mapping[str, str] = field(default_factory=dict)
    cwd: Path | None = None
    stop_timeout_seconds: float = 3.0

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("service name must not be empty")
        command = tuple(self.command)
        if not command or any(not isinstance(item, str) or not item for item in command):
            raise ValueError("service command must contain at least one non-empty string")
        if isinstance(self.restart_policy, str):
            object.__setattr__(self, "restart_policy", RestartPolicy.parse(self.restart_policy))
        if type(self.max_restarts) is not int or self.max_restarts < 0:
            raise ValueError("max_restarts must be a non-negative integer")
        if not _is_non_negative_finite(self.restart_delay_seconds):
            raise ValueError("restart_delay_seconds must be finite and non-negative")
        if not _is_non_negative_finite(self.stop_timeout_seconds):
            raise ValueError("stop_timeout_seconds must be finite and non-negative")
        object.__setattr__(self, "command", command)
        object.__setattr__(self, "restart_delay_seconds", float(self.restart_delay_seconds))
        object.__setattr__(self, "stop_timeout_seconds", float(self.stop_timeout_seconds))
        object.__setattr__(self, "env", dict(self.env))
        if self.cwd is not None:
            object.__setattr__(self, "cwd", Path(self.cwd))

    @property
    def argv(self) -> tuple[str, ...]:
        return self.command


@dataclass(frozen=True, slots=True)
class AppConfig:
    """Composition inputs for the manual foreground supervisor."""

    state_dir: Path
    services: tuple[ServiceSpec, ...] = ()
    poll_interval_seconds: float = 0.05
    child_stop_timeout_seconds: float = 3.0
    profile_registry: AgentProfileRegistry | None = None
    foreground_configs: tuple[tuple[str, Path], ...] = ()

    def __post_init__(self) -> None:
        state_dir = Path(self.state_dir)
        services = tuple(self.services)
        if len({service.name for service in services}) != len(services):
            raise ValueError("service names must be unique")
        if any(not isinstance(service, ServiceSpec) for service in services):
            raise TypeError("services must contain ServiceSpec values")
        if (
            self.profile_registry is not None
            and type(self.profile_registry) is not AgentProfileRegistry
        ):
            raise TypeError("profile_registry must be an AgentProfileRegistry or None")
        if not _is_positive_finite(self.poll_interval_seconds):
            raise ValueError("poll_interval_seconds must be finite and positive")
        if type(self.foreground_configs) is not tuple:
            raise TypeError("foreground_configs must be immutable pairs")
        agents = set()
        for entry in self.foreground_configs:
            if (type(entry) is not tuple or len(entry) != 2 or type(entry[0]) is not str
                    or not isinstance(entry[1], Path) or self.profile_registry is None
                    or entry[0] in agents):
                raise ValueError("foreground configuration binding invalid")
            self.profile_registry.profile(entry[0])
            agents.add(entry[0])
        if not _is_non_negative_finite(self.child_stop_timeout_seconds):
            raise ValueError("child_stop_timeout_seconds must be finite and non-negative")
        object.__setattr__(self, "state_dir", state_dir)
        object.__setattr__(self, "services", services)

    @property
    def state_path(self) -> Path:
        return self.state_dir / "state.json"

    @property
    def control_path(self) -> Path:
        return self.state_dir / "control.json"

    @property
    def lease_path(self) -> Path:
        return self.state_dir / "supervisor.lock"


def _is_non_negative_finite(value: object) -> bool:
    if type(value) not in (int, float):
        return False
    numeric = float(cast(int | float, value))
    return math.isfinite(numeric) and numeric >= 0


def _is_positive_finite(value: object) -> bool:
    return _is_non_negative_finite(value) and float(cast(int | float, value)) > 0
