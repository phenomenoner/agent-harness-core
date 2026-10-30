"""Explicit foreground workspace composition; unavailable backend preserves input."""

import json
from math import isfinite
from pathlib import Path

from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.runtime_service import RuntimeService
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.turns import _sha256_hex
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

from .foreground_memory import compose_foreground_memory


def read_foreground_config(path):
    from .runtime import _json

    with Path(path).open("rb") as stream:
        raw = stream.read(65537)
    try:
        config = _json(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise ContractError("invalid foreground configuration") from None
    return config


def compose_foreground(path, *, profile_registry=None):
    return compose_foreground_document(
        read_foreground_config(path), profile_registry=profile_registry)


def compose_foreground_document(config, *, profile_registry=None):
    from .foreground_profiles import require_profile_agent

    require_profile_agent(config, profile_registry)
    if type(config) is dict and config.get("schema_version") in (
            "ahc.foreground.config.v2", "ahc.foreground.config.v3"):
        from .foreground_trusted import compose_trusted_foreground

        return compose_trusted_foreground(config, profile_registry=profile_registry)
    from ahc.adapters.docker_workspace import DockerRunner

    fields = {
        "schema_version",
        "runtime_root",
        "session",
        "workspace_id",
        "docker_executable",
        "docker_client_config",
        "checkpoint_digest",
        "backend",
    }
    if (
        type(config) is not dict
        or not fields <= set(config)
        or set(config) - fields - {"skill_root"}
    ):
        raise ContractError("invalid foreground configuration fields")
    if config["schema_version"] != "ahc.foreground.config.v1":
        raise ContractError("unsupported foreground configuration version")
    backend = config["backend"]
    if backend != "unavailable":
        if type(backend) is not dict or set(backend) != {
            "kind",
            "enabled",
            "host_executable",
            "distribution",
            "launch_spec",
            "model",
            "turn_timeout",
            "lifetime_seconds",
        }:
            raise ContractError("invalid foreground backend fields")
        if backend["kind"] != "codex-wsl-offline" or type(backend["enabled"]) is not bool:
            raise ContractError("unsupported foreground backend or activation")
        timeout = backend["turn_timeout"]
        if type(timeout) not in (int, float) or not isfinite(timeout) or not 0 < timeout <= 300:
            raise ContractError("invalid foreground turn timeout")
        if (
            type(backend["model"]) is not str
            or not backend["model"]
            or len(backend["model"]) > 4096
        ):
            raise ContractError("invalid foreground model")
    for key in ("runtime_root", "docker_executable", "docker_client_config"):
        if type(config[key]) is not str or not Path(config[key]).is_absolute():
            raise ContractError("foreground paths must be explicit and absolute")
    session = VirtualSessionIdentity.from_dict(config["session"])
    workspace_id = _identifier(config["workspace_id"], "workspace")
    checkpoint = config["checkpoint_digest"]
    if checkpoint is not None:
        _sha256_hex(checkpoint)
    selector = None
    if config.get("skill_root") is not None:
        from ahc.application.skill_service import SkillTurnService
        from ahc.infrastructure.skill_repository import SkillRepository

        if type(config["skill_root"]) is not str:
            raise ContractError("foreground skill root must be an absolute path")
        selector = SkillTurnService(SkillRepository(config["skill_root"]))
    store = RuntimeRepository(config["runtime_root"])
    current = store.read_current(session.ref.virtual_session_id)
    if current.session != session or current.status != "active":
        raise ContractError("foreground session must be current and active")
    runner = DockerRunner(config["docker_executable"], client_config=config["docker_client_config"])
    inbox = RuntimeInbox(store)

    factory = None
    if type(backend) is dict:
        from .wsl_codex_backend import WslCodexBackendFactory

        factory = WslCodexBackendFactory(
            store,
            backend["host_executable"],
            backend["distribution"],
            launch_spec=backend["launch_spec"],
            model=backend["model"],
            lifetime_seconds=backend["lifetime_seconds"],
        )

    def ingress(expected, _tools):
        # Disabled configuration observes database failures without admitting input.
        inbox.next_pending(expected.session)
        return None

    def unavailable(*_args, **_kwargs):
        raise ContractError("foreground backend unavailable")

    selected = unavailable
    backend_name = "unavailable"
    if factory is not None and backend["enabled"]:
        from ahc.application.local_runtime_ingress import LocalRuntimeIngress

        def ingress(expected, tools):
            artifact, configuration = factory.binding(tools)
            return LocalRuntimeIngress(
                inbox,
                artifact_digest=artifact,
                configuration_digest=lambda _tools: configuration,
                workspace_scope=workspace_id,
                turn_timeout=backend["turn_timeout"],
            )(expected, tools)

        selected = factory
        backend_name = "codex-wsl-offline"

    def open_workspace(expected):
        if factory is not None and backend["enabled"]:
            factory.recover(expected.session, workspace_id)
        return RuntimeService(store, None).open_docker_workspace(
            expected, workspace_id, runner, checkpoint_digest=checkpoint
        )

    runtime = ForegroundRuntime(
        store,
        session,
        ingress,
        selected,
        selector=selector,
        open_workspace=open_workspace,
        memory=compose_foreground_memory(),
    )
    runtime.backend_name = backend_name
    return runtime
