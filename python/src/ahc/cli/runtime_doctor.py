"""Explicit read-only runtime inspection; never initialize or repair a store."""

from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_health import inspect_runtime_health
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingRepositoryError

from .foreground_config_native import parse_trusted_foreground_config


def inspect_runtime_document(document):
    config = parse_trusted_foreground_config(document)
    scope = {"observation_scope": "persisted-state-counts", "live_probe": False}
    try:
        if not Path(config.runtime_root).exists():
            return {**scope, "status": "absent"}
        store = RuntimeRepository(config.runtime_root)
        current = store.read_current(config.session.ref.virtual_session_id)
        if current.session != config.session:
            return {**scope, "status": "blocked", "reason_code": "session-identity-mismatch"}
        health = inspect_runtime_health(store, config.session)
    except (OSError, WorkingRepositoryError):
        return {**scope, "status": "indeterminate", "reason_code": "runtime-unreadable"}
    degraded = any(value["status"] == "unreadable" for value in health["domains"].values())
    return {**scope, "status": "degraded" if degraded else "observed",
            "domains": health["domains"]}


def inspect_selected_runtime(args, document):
    runtime_report = {}
    from .foreground_selection import select_foreground

    selected = select_foreground(args, document)
    if args.inspect_runtime and selected is None:
        raise ContractError("runtime inspection requires a selected foreground configuration")
    if selected is not None:
        from .foreground_memory import inspect_foreground_document

        runtime_report = {"runtime": inspect_foreground_document(
            selected, profile_registry=document.profile_registry)}
        if args.inspect_runtime:
            from ahc.application.native_memory_diagnostics import inspect_native_provider
            from ahc.application.workspace_availability import inspect_workspace_launcher

            from .channel_diagnostics import inspect_channel_credentials
            from .foreground_native_memory import read_native_binding

            config = parse_trusted_foreground_config(selected)
            binding = read_native_binding(config, document.profile_registry)
            if binding is not None:
                runtime_report["runtime"]["native_memory"]["source"] = inspect_native_provider(
                    binding)
            runtime_report["runtime"]["persistence"] = inspect_runtime_document(selected)
            runtime_report["runtime"]["channel_credentials"] = inspect_channel_credentials(
                config)
            if config.backend["enabled"]:
                runtime_report["runtime"]["workspace"] = inspect_workspace_launcher(
                    config.workspace["interpreter"] if config.backend["kind"] == "codex-linux-trusted"
                    else config.workspace["wsl_executable"])
    return runtime_report
