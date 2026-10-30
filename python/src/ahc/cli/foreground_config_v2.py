"""Strict, side-effect-free parsing for the trusted foreground configuration."""
from __future__ import annotations

import json
import math
import posixpath
import re
import unicodedata
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath

from ahc.adapters.codex_reasoning import effort_options
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _sha256_hex

from .foreground_agent_context import parse_agent_context, preview_agent_context
from .foreground_batch import parse_batch
from .foreground_channels import parse_channel_options, preview_channels
from .foreground_child_limits import parse_child_limits
from .foreground_mcp import parse_mcp, preview_mcp
from .foreground_memory import parse_memory
from .foreground_memory_pack import preview_memory
from .foreground_native import parse_native, preview_native
from .foreground_native_memory import parse_native_memory_reference

FOREGROUND_CONFIG_V2_SCHEMA_VERSION = "ahc.foreground.config.v2"
FOREGROUND_CONFIG_SCHEMA_VERSION = FOREGROUND_CONFIG_V2_SCHEMA_VERSION

_TOP_REQUIRED = frozenset({
        "schema_version", "runtime_root", "session", "workspace_id",
        "checkpoint_digest",
        "workspace",
        "backend",
        "policy",
})
_TOP_ALLOWED = _TOP_REQUIRED | {
    "skill_root", "batch", "mcp", "channels", "telegram_polling", "discord_gateway", "memory",
    "native_minion", "child_limits", "native_memory", "agent_context", "channel_control",
    "host_paths", "operating_context", "learning", "symbolic_canvas", "execution_profile"}
_WORKSPACE_FIELDS = frozenset(
    {
        "kind",
        "wsl_executable",
        "distribution",
        "user",
        "cwd",
        "interpreter",
        "execution_timeout",
        "lifetime_seconds",
    }
)
_BACKEND_FIELDS = frozenset(
    {"kind", "enabled", "bundle", "codex_home", "model", "turn_timeout", "lifetime_seconds"}
)
_POLICY_FIELDS = frozenset(
    {"revision", "approval_mode", "sandbox_mode", "workspace_granted", "approval_wait_seconds"}
)
_DISTRIBUTION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_DEDICATED_DISTRIBUTION = "AHC-Codex-Ubuntu-24.04"
_LIFETIME_MIN = 1
_LIFETIME_MAX = 3600


def _object(value: object, fields: frozenset[str], label: str) -> dict[str, object]:
    if type(value) is not dict:
        raise ContractError(f"{label} must be an object")
    keys = set(value)
    if any(type(key) is not str for key in keys) or keys != fields:
        raise ContractError(f"{label} fields are invalid")
    return {key: value[key] for key in fields}


def _top_object(value: object) -> dict[str, object]:
    if type(value) is not dict:
        raise ContractError("foreground configuration must be an object")
    keys = set(value)
    if any(type(key) is not str for key in keys) or not _TOP_REQUIRED <= keys <= _TOP_ALLOWED:
        raise ContractError("foreground configuration fields are invalid")
    return {key: value[key] for key in keys}


def _safe_text(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ContractError(f"{label} must be a non-empty string")
    if any(
        ord(char) < 32 or unicodedata.category(char) == "Cc" or 0xD800 <= ord(char) <= 0xDFFF
        for char in value
    ):
        raise ContractError(f"{label} contains an invalid control or surrogate character")
    return value


def _host_path(value: object, label: str) -> str:
    path = _safe_text(value, label)
    if not PureWindowsPath(path).is_absolute():
        raise ContractError(f"{label} must be an absolute Windows host path")
    return path


def _posix_path(value: object, label: str, *, executable: bool = False) -> str:
    path = _safe_text(value, label)
    if (
        not path.startswith("/")
        or path.startswith("//")
        or "\\" in path
        or posixpath.normpath(path) != path
        or PurePosixPath(path).as_posix() != path
        or (executable and path == "/")
    ):
        raise ContractError(f"{label} must be an absolute normalized POSIX path")
    return path


def _native_path(value: object, label: str, *, executable: bool = False) -> str:
    path = _posix_path(value, label, executable=executable)
    if path == "/" or path == "/mnt" or path.startswith("/mnt/"):
        raise ContractError(f"{label} requires a Linux-owned location outside Windows mounts")
    return path


def _finite_positive(value: object, label: str) -> int | float:
    if type(value) not in (int, float):
        raise ContractError(f"{label} must be a finite positive number")
    try:
        numeric = float(value)
    except (OverflowError, TypeError, ValueError):
        raise ContractError(f"{label} must be a finite positive number") from None
    if not math.isfinite(numeric) or numeric <= 0:
        raise ContractError(f"{label} must be a finite positive number")
    return value


def _lifetime(value: object, label: str, *, native=False) -> int:
    from ahc.domain.execution_budgets import NATIVE_LIFETIME_MAX_SECONDS

    maximum = NATIVE_LIFETIME_MAX_SECONDS if native else _LIFETIME_MAX
    if type(value) is not int or not _LIFETIME_MIN <= value <= maximum:
        raise ContractError(f"{label} must be an integer in [1, {maximum}]")
    return value


def _distribution(value: object) -> str:
    if type(value) is not str or not _DISTRIBUTION_RE.fullmatch(value):
        raise ContractError("workspace.distribution must be an identifier-like string")
    return value


def _model(value: object) -> str:
    if type(value) is not str or not value or len(value) > 4096:
        raise ContractError("backend.model must contain 1 to 4096 characters")
    return value


def _parse_workspace(value: object, *, native=False) -> dict[str, object]:
    extra = {key for key in ("lifetime_mode", "protection")
             if type(value) is dict and key in value}
    if "protection" in extra and not native:
        raise ContractError("workspace.protection requires native Linux configuration")
    fields = (
        _WORKSPACE_FIELDS - {"wsl_executable", "distribution", "user"}
        if native else _WORKSPACE_FIELDS
    )
    raw = _object(value, fields | extra, "workspace")
    kinds = ("linux-python", "linux-ipython") if native else ("wsl-python", "wsl-ipython")
    if raw["kind"] not in kinds:
        raise ContractError("workspace.kind is unsupported")
    launch = {}
    if not native:
        if raw["user"] != "ahc":
            raise ContractError("workspace.user must be ahc")
        distribution = _distribution(raw["distribution"])
        if distribution != _DEDICATED_DISTRIBUTION:
            raise ContractError("workspace.distribution must be the dedicated AHC distribution")
        launch = {"wsl_executable": _host_path(raw["wsl_executable"], "workspace.wsl_executable"),
                  "distribution": distribution, "user": "ahc"}
    path = _native_path if native else _posix_path
    workspace = {
        "kind": raw["kind"],
        **launch,
        "cwd": path(raw["cwd"], "workspace.cwd"),
        "interpreter": path(raw["interpreter"], "workspace.interpreter", executable=True),
        "execution_timeout": _finite_positive(
            raw["execution_timeout"], "workspace.execution_timeout"
        ),
        "lifetime_seconds": _lifetime(raw["lifetime_seconds"], "workspace.lifetime_seconds",
                                      native=native),
    }
    if workspace["execution_timeout"] > workspace["lifetime_seconds"]:
        raise ContractError("workspace.execution_timeout exceeds lifetime_seconds")
    if "lifetime_mode" in extra:
        if raw["lifetime_mode"] not in ("bounded", "foreground"):
            raise ContractError("workspace.lifetime_mode is unsupported")
        workspace["lifetime_mode"] = raw["lifetime_mode"]
    if "protection" in extra:
        if workspace["kind"] not in ("linux-python", "linux-ipython"):
            raise ContractError("workspace.protection requires native Python")
        from .foreground_workspace_protection import parse_native_protection
        workspace["protection"] = parse_native_protection(raw["protection"], workspace["cwd"])
    return workspace


def _parse_backend(value: object, *, native=False) -> dict[str, object]:
    extra = set()
    policies = {key for key in ("context_policy", "overload_recovery", "time_budget")
                if type(value) is dict and key in value}
    if policies and not native:
        raise ContractError("backend recovery policies require native Linux configuration")
    extra.update(policies)
    budgets = {key for key in ('startup_timeout', 'request_timeout', 'settlement_timeout',
                              'wrap_up_timeout')
               if type(value) is dict and key in value}
    if budgets and not native:
        raise ContractError('backend phase budgets require native Linux configuration')
    extra.update(budgets)
    if type(value) is dict and "reasoning_effort" in value:
        extra.add("reasoning_effort")
    if type(value) is dict and "service_tier" in value:
        extra.add("service_tier")
    if type(value) is dict and "protection" in value:
        if not native:
            raise ContractError("backend.protection requires native Linux configuration")
        extra.add("protection")
    if type(value) is dict and "authentication" in value:
        if not native or "protection" not in value:
            raise ContractError("backend.authentication requires protected native Linux")
        extra.add("authentication")
    raw = _object(value, _BACKEND_FIELDS | extra, "backend")
    kind = "codex-linux-trusted" if native else "codex-wsl-trusted"
    if raw["kind"] != kind:
        raise ContractError("backend.kind is unsupported")
    if type(raw["enabled"]) is not bool:
        raise ContractError("backend.enabled must be a boolean")
    path = _native_path if native else _posix_path
    backend = {
        "kind": kind,
        "enabled": raw["enabled"],
        "bundle": path(raw["bundle"], "backend.bundle"),
        "codex_home": path(raw["codex_home"], "backend.codex_home"),
        "model": _model(raw["model"]),
        "turn_timeout": _finite_positive(raw["turn_timeout"], "backend.turn_timeout"),
        "lifetime_seconds": _lifetime(raw["lifetime_seconds"], "backend.lifetime_seconds",
                                      native=native),
    }
    if backend["turn_timeout"] > backend["lifetime_seconds"]:
        raise ContractError("backend.turn_timeout exceeds lifetime_seconds")
    for key in budgets:
        backend[key] = _finite_positive(raw[key], 'backend.' + key)
        if backend[key] > backend['lifetime_seconds']:
            raise ContractError('backend phase timeout exceeds lifetime_seconds')
    if backend['turn_timeout'] + backend.get('settlement_timeout', 0) > backend['lifetime_seconds']:
        raise ContractError('backend lifetime does not cover terminal settlement')
    backend.update(effort_options(raw))
    if "service_tier" in extra:
        if raw["service_tier"] != "priority":
            raise ContractError("backend.service_tier is unsupported")
        backend["service_tier"] = "priority"
    if "protection" in extra:
        from .foreground_backend_protection import parse_backend_protection

        backend["protection"] = parse_backend_protection(raw["protection"])
    if "authentication" in extra:
        if raw["authentication"] != "staged-codex-file":
            raise ContractError("backend.authentication is unsupported")
        backend["authentication"] = "staged-codex-file"
    if "context_policy" in policies:
        from ahc.adapters.codex_context_policy import context_policy

        backend["context_policy"] = context_policy(raw["context_policy"])
        if backend["context_policy"] is None:
            raise ContractError("backend context policy cannot be null")
    if "overload_recovery" in policies:
        from ahc.domain.backend_recovery import recovery_policy

        backend["overload_recovery"] = recovery_policy(raw["overload_recovery"])
        if backend["overload_recovery"] is None:
            raise ContractError("backend recovery policy cannot be null")
    if "time_budget" in policies:
        from ahc.domain.adaptive_time_budget import time_budget_policy
        backend["time_budget"] = time_budget_policy(raw["time_budget"])
    return backend


def _parse_policy(value: object, workspace: dict[str, object]) -> EffectivePolicy:
    raw = _object(value, _POLICY_FIELDS, "policy")
    if type(raw["revision"]) is not int or raw["revision"] < 1:
        raise ContractError("policy.revision must be a positive integer")
    if raw["approval_mode"] not in ("manual", "auto-approve", "always-approve", "yolo"):
        raise ContractError("policy.approval_mode is unsupported")
    if raw["sandbox_mode"] not in ("read-only", "workspace-write", "danger-full-access"):
        raise ContractError("policy.sandbox_mode is unsupported")
    if type(raw["workspace_granted"]) is not bool:
        raise ContractError("policy.workspace_granted must be a boolean")
    approval_wait = _finite_positive(raw["approval_wait_seconds"], "policy.approval_wait_seconds")
    return EffectivePolicy(
        raw["revision"],
        raw["approval_mode"],
        raw["sandbox_mode"],
        raw["workspace_granted"],
        digest_json(workspace),
        approval_wait,
    )


@dataclass(frozen=True, slots=True)
class TrustedForegroundConfig:
    """Immutable validated v2 values with fresh read-only projections."""

    runtime_root: str
    session: VirtualSessionIdentity
    workspace_id: str
    checkpoint_digest: str | None
    skill_root: str | None
    effective_policy: EffectivePolicy
    configuration_digest: str
    _workspace_json: str
    _backend_json: str
    _config_json: str

    @property
    def workspace(self) -> dict[str, object]:
        return json.loads(self._workspace_json)
    @property
    def backend(self) -> dict[str, object]:
        return json.loads(self._backend_json)
    def to_dict(self) -> dict[str, object]:
        """Return a fresh canonical v2 input projection."""
        return json.loads(self._config_json)

    def preview(self) -> dict[str, object]:
        """Return prospective values without activation or private backend homes."""
        backend = self.backend
        return {
            "workspace": self.workspace,
            "backend": {key: backend[key] for key in ("kind", "enabled", "model")},
            "effective_policy": self.effective_policy.to_dict(),
            **({"execution_profile": self.to_dict()["execution_profile"]}
               if "execution_profile" in self.to_dict() else {}),
            **({"batch": self.to_dict()["batch"]} if "batch" in self.to_dict() else {}),
            **preview_mcp(self),
            **preview_channels(self),
            **preview_memory(self), **preview_native(self),
            **preview_agent_context(self),
            **({"learning": {
                "mode": self.to_dict()["learning"]["mode"],
                "isolated_evaluation_configured": "evaluation" in self.to_dict()["learning"],
                "automatic_configured": self.to_dict()["learning"]["mode"] == "reversible-trials",
                "automatic_enabled": False,
                "observation_scope": "configuration-only",
            }} if "learning" in self.to_dict() else {}),
            **parse_child_limits(self.to_dict()),
            **({"symbolic_canvas": self.to_dict()["symbolic_canvas"]}
               if "symbolic_canvas" in self.to_dict() else {}),
        }

def parse_foreground_config_v2(value: object) -> TrustedForegroundConfig:
    """Validate one complete v2 configuration without filesystem or runtime effects."""
    return _parse_trusted(value, native=False)


def _parse_trusted(value, *, native):
    raw = _top_object(value)
    schema = "ahc.foreground.config.v3" if native else FOREGROUND_CONFIG_V2_SCHEMA_VERSION
    if raw["schema_version"] != schema:
        raise ContractError("unsupported foreground configuration version")
    from .foreground_user_access import parse_user_access

    user_access = parse_user_access(raw, native=native)
    path = _native_path if native else _host_path
    runtime_root = path(raw["runtime_root"], "runtime_root")
    session = VirtualSessionIdentity.from_dict(raw["session"])
    workspace_id = _identifier(raw["workspace_id"], "workspace")
    checkpoint = raw["checkpoint_digest"]
    if checkpoint is not None:
        _sha256_hex(checkpoint)
    skill_root = raw.get("skill_root")
    if skill_root is not None:
        skill_root = path(skill_root, "skill_root")
    workspace = _parse_workspace(raw["workspace"], native=native)
    if "protection" in workspace and runtime_root not in workspace["protection"]["protected_roots"]:
        raise ContractError("workspace.protection must explicitly hide runtime_root")
    backend = _parse_backend(raw["backend"], native=native)
    if (backend.get("time_budget", {}).get("enabled")
            and workspace.get("lifetime_mode") != "foreground"):
        raise ContractError("adaptive time budget requires foreground-owned workspace lifetime")
    if (backend.get("time_budget", {}).get("enabled") and "protection" in backend
            and raw.get("execution_profile") != "user-full-access"):
        raise ContractError("adaptive time budget cannot bypass protected ephemeral process lifetime")
    effective_policy = _parse_policy(raw["policy"], workspace)
    document = {
        "schema_version": schema,
        "runtime_root": runtime_root,
        "session": session.to_dict(),
        "workspace_id": workspace_id,
        "checkpoint_digest": checkpoint,
        "workspace": workspace,
        "backend": backend,
        "policy": {key: raw["policy"][key] for key in _POLICY_FIELDS},
        "skill_root": skill_root,
    }
    document.update(user_access)
    document.update(parse_batch(raw, session))
    from .foreground_learning import parse_learning

    document.update(parse_learning(raw))
    from .foreground_canvas import parse_canvas

    document.update(parse_canvas(raw))
    from .foreground_host_paths import parse_host_context

    document.update(parse_host_context(raw))
    document.update(parse_agent_context(raw))
    document.update(parse_native_memory_reference(raw))
    document.update(parse_child_limits(raw))
    document.update(parse_mcp(raw, session))
    document.update({**parse_memory(raw), **parse_native(raw, session)})
    document.update(parse_channel_options(raw, session))
    from .foreground_control_config import parse_channel_control

    document.update(parse_channel_control(raw, session, backend))
    from .foreground_backend_protection import require_protected_routes

    require_protected_routes(document)
    try:
        config_json = canonical_json(document).decode("utf-8")
        workspace_json = canonical_json(workspace).decode("utf-8")
        backend_json = canonical_json(backend).decode("utf-8")
        configuration_digest = digest_json(
            {**document, "effective_policy": effective_policy.to_dict()}
        )
    except (UnicodeError, ContractError):
        raise ContractError("foreground configuration is not canonical JSON") from None
    return TrustedForegroundConfig(
        runtime_root, session, workspace_id, checkpoint, skill_root,
        effective_policy, configuration_digest, workspace_json, backend_json, config_json,
    )
__all__ = ("TrustedForegroundConfig", "parse_foreground_config_v2")
