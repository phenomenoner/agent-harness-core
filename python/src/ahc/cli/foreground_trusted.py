"""Explicit v2 composition, preserving a disabled no-process standby path."""

from pathlib import Path

from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_reply_catalog import ChannelReplyTools
from ahc.infrastructure.foreground_schedule import ForegroundSchedule
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy

from .foreground_batch import compose_batch
from .foreground_channels import compose_channels
from .foreground_children import ChildEnabledBackend, ForegroundChildren
from .foreground_discord import compose_discord
from .foreground_files import compose_files
from .foreground_mcp import compose_mcp
from .foreground_memory import compose_foreground_memory
from .foreground_memory_pack import compose_memory_pack, compose_memory_search
from .foreground_progress import compose_progress
from .foreground_telegram import compose_telegram
from .foreground_typing import compose_typing
from .trusted_codex_backend import TrustedCodexBackendFactory


def compose_trusted_foreground(value, *, profile_registry=None, _shared_store=None,
                               _background_controls=None):
    from .foreground_config_native import parse_trusted_foreground_config
    from .foreground_profiles import bind_profile_configuration

    config = bind_profile_configuration(parse_trusted_foreground_config(value), profile_registry)
    if config.backend["kind"] == "codex-linux-trusted":
        from ahc.infrastructure.native_state_path import require_native_state_directory
        require_native_state_directory(config.runtime_root)
    memory_pack = compose_memory_pack(config, profile_registry)
    memory_search = compose_memory_search(config, profile_registry)
    repository_type = RuntimeRepository
    if config.backend["enabled"]:
        from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository

        repository_type = ThreadedRuntimeRepository
    store = _shared_store if _shared_store is not None else repository_type(config.runtime_root)
    if store.root.resolve() != Path(config.runtime_root).resolve():
        raise ContractError("background workbench runtime root differs")
    from .foreground_native_memory import compose_native_memory

    native_memory = compose_native_memory(config, profile_registry, store)
    current = store.read_current(config.session.ref.virtual_session_id)
    if current.session != config.session or current.status != "active":
        raise ContractError("trusted foreground session must be current and active")
    inbox = RuntimeInbox(store)

    def standby(expected, _tools):
        inbox.next_pending(expected.session)
        return None

    def unavailable(*_args, **_kwargs):
        raise ContractError("trusted foreground is disabled")

    if not config.backend["enabled"]:
        runtime = ForegroundRuntime(
            store, config.session, standby, unavailable, memory=compose_foreground_memory(config)
        )
        runtime.backend_name = "unavailable"
        return runtime
    config.effective_policy.require_workspace_grant(digest_json(config.workspace))
    if config.to_dict().get("child_limits", {}).get("max_artifacts", 0):
        from ahc.infrastructure.child_artifact_schema import prepare_child_artifact_schema
        prepare_child_artifact_schema(store, session=config.session, workspace_id=config.workspace_id)
    channels = compose_channels(config, store)
    controls = None
    if config.to_dict().get("channel_control", {}).get("enabled"):
        from ahc.infrastructure.channel_control_store import ChannelControlStore

        controls = ChannelControlStore(store, config)
        store.channel_controls = controls
    background_enabled = controls is not None and _background_controls is None
    telegram = compose_telegram(config, store, concurrent=controls is not None)
    discord = compose_discord(config, store)
    backend = TrustedCodexBackendFactory(store, config)
    children = ForegroundChildren(store, backend)
    batch = compose_batch(config, store)
    active_cancel = [lambda: False]
    selected_backend = [backend]
    mcp = compose_mcp(config, store, cancelled=lambda: active_cancel[0]())
    selector = None
    if config.skill_root is not None:
        from ahc.application.skill_service import SkillTurnService
        from ahc.infrastructure.skill_repository import SkillRepository

        selector = SkillTurnService(SkillRepository(config.skill_root))

    def open_workspace(expected):
        from ahc.adapters.wsl_python_workspace import WslPythonWorkspace
        from ahc.adapters.wsl_stream_runner import WslStreamRunner
        from ahc.application.workspace_availability import require_workspace_launcher

        native = config.backend["kind"] == "codex-linux-trusted"
        require_workspace_launcher(config.workspace["interpreter"] if native
                                   else config.workspace["wsl_executable"])
        backend.recover(expected.session, config.workspace_id)
        workspace = config.workspace
        options = {"owner_lifetime": True} if workspace.get("lifetime_mode") == "foreground" else {}
        workspace_type = WslPythonWorkspace
        if native:
            from ahc.adapters.native_process_runner import NativeProcessRunner
            from ahc.adapters.native_python_workspace import NativePythonWorkspace
            from .foreground_workspace_protection import native_workspace_protection
            from .foreground_user_environment import native_workspace_environment
            runner = NativeProcessRunner(cwd=workspace["cwd"],
                                         environment=native_workspace_environment(config.to_dict()),
                                         persistent=True, **options,
                                         **native_workspace_protection(workspace))
            workspace_type = NativePythonWorkspace
        else:
            runner = WslStreamRunner(
                workspace["wsl_executable"], workspace["distribution"], persistent=True, **options)

        def create(identity, checkpoint):
            return workspace_type(
                store,
                runner,
                identity,
                effective_policy=config.effective_policy,
                environment_digest=digest_json(workspace),
                cwd=workspace["cwd"],
                interpreter=workspace["interpreter"],
                user=workspace.get("user"),
                timeout=workspace["execution_timeout"],
                lifetime_seconds=workspace["lifetime_seconds"],
                checkpoint=checkpoint,
                cancelled=lambda: active_cancel[0](),
                batch=batch,
                engine="ipython" if workspace["kind"] in ("wsl-ipython", "linux-ipython") else "python",
            )

        return RuntimeWorkspace.open(
            store, expected, config.workspace_id, create, config.checkpoint_digest
        )

    def ingress(expected, tools):
        chosen = config
        factory = backend
        turn_inbox = inbox
        preference_controls = controls or _background_controls
        if preference_controls is not None:
            try:
                pending = inbox.next_pending(expected.session, exclude_scheduled=background_enabled)
                if pending is None:
                    return None
                source_operation = pending["operation_id"]
                if _background_controls is not None:
                    from ahc.infrastructure.background_schedule_dispatch import BackgroundScheduleDispatch

                    with store._read() as connection:
                        origin = BackgroundScheduleDispatch.origin_route(
                            connection, expected.session, source_operation)
                        row = connection.execute("SELECT source_operation_id FROM "
                            "runtime_background_dispatch WHERE worker_operation_id=?",
                            (source_operation,)).fetchone()
                    if origin not in preference_controls.routes:
                        raise ContractError("background origin route is not enabled")
                    source_operation = row[0]
                preferences = preference_controls.preferences_for(source_operation)
            except WorkingWriterBusy:
                return None  # Only selection reads; no factory or turn admission yet.
            from types import SimpleNamespace

            turn_inbox = SimpleNamespace(store=store, next_pending=lambda _session: pending)
            if preferences is not None:
                document = config.to_dict()
                document["backend"]["model"] = preferences["model"]
                if preferences["effort"] is not None:
                    document["backend"]["reasoning_effort"] = preferences["effort"]
                if preferences["fast"]:
                    document["backend"]["service_tier"] = "priority"
                else:
                    document["backend"].pop("service_tier", None)
                document["policy"]["approval_mode"] = preferences["approval"]
                document["policy"]["revision"] += preferences["revision"]
                chosen = bind_profile_configuration(parse_trusted_foreground_config(document),
                                                    profile_registry)
            from ahc.application.channel_tool_approval import ChannelToolGate
            from ahc.application.pending_approval import PendingApprovalPort
            from ahc.infrastructure.channel_presentation import background_operation
            from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore

            factory = TrustedCodexBackendFactory(
                store, chosen, tool_gate=ChannelToolGate,
                approval_port_factory=lambda admitted: PendingApprovalPort(
                    RuntimeApprovalStore(store), admitted, notifier=preference_controls.notify_approval,
                    interactive=(_background_controls is None and not background_operation(
                        store, admitted.operation_id))))
        selected_backend[0] = factory
        artifact, configuration = factory.binding(tools)
        request = LocalRuntimeIngress(
            turn_inbox,
            artifact_digest=artifact,
            configuration_digest=lambda _tools: configuration,
            workspace_scope=config.workspace_id,
            turn_timeout=config.backend["turn_timeout"],
            effective_policy=chosen.effective_policy,
        )(expected, tools)
        if request is None:
            return None
        from .foreground_time_budget import bind_time_budget
        return bind_time_budget(factory, request, tools)

    def select_backend(request, tools, *, cancelled=lambda: False):
        outer_cancel = cancelled
        if controls is not None:
            def cancelled():
                return outer_cancel() or controls.cancelled(request.operation_id)
        active_cancel[0] = cancelled
        factory = selected_backend[0]
        children.owner = factory
        return ChildEnabledBackend(factory(request, tools, cancelled=cancelled), children,
                                   request, tools.workspace, cancelled)

    from ahc.application.workspace_schedule import WorkspaceScheduleTools

    from .foreground_commands import compose_commands
    from .foreground_learning import compose_learning
    from .foreground_canvas import compose_canvas

    receiver = discord
    if controls is not None:
        from .foreground_control_pump import ChannelControlPump

        receiver = ChannelControlPump(controls, telegram, discord)
    runtime = ForegroundRuntime(
        store,
        config.session,
        ingress,
        select_backend,
        open_workspace=open_workspace,
        selector=selector,
        lazy_skill_disclosure=True,
        learning=compose_learning(config, selector, store=store),
        canvas=compose_canvas(config, store),
        memory=compose_foreground_memory(config),
        memory_pack=memory_pack,
        memory_search=memory_search,
        native_memory=native_memory,
        scheduling=(WorkspaceScheduleTools(controls or _background_controls)
                    if controls is not None or _background_controls is not None else None),
        artifacts=ArtifactRepository(store),
        schedule=(ForegroundSchedule(store) if not background_enabled
                  and _background_controls is None else None),
        mcp=mcp,
        channel_delivery=channels,
        channel_ingress=telegram if controls is None else None,
        channel_receiver=receiver,
        channel_typing=compose_typing(config, store),
        progress=compose_progress(config, store, cancelled=lambda: active_cancel[0]()),
        channel_files=compose_files(config, store),
        children=children,
        local_reply=compose_commands(config, store),
        channel_replies=ChannelReplyTools(store) if channels is not None else None,
    )
    runtime.backend_name = config.backend["kind"]
    if background_enabled:
        from ahc.application.background_schedule import BackgroundSchedule

        def background_runtime(binding):
            document = config.to_dict()
            document.update(session=binding["worker_session"].to_dict(),
                            workspace_id=binding["workspace_id"], checkpoint_digest=None)
            for option in ("channel_control", "telegram_polling", "discord_gateway"):
                document.pop(option, None)
            for channel in document.get("channels", []):
                channel["route"]["session"] = binding["worker_session"].to_dict()
            return compose_trusted_foreground(document, profile_registry=profile_registry,
                _shared_store=store, _background_controls=controls)

        runtime.background_schedule = BackgroundSchedule(store, config.session, background_runtime)
        # The ordinary status reader must not pause scheduling. Restart preparation
        # is invoked only after the durable restart command has been delivered.
        controls.bind_background_status(runtime.background_schedule.status)
        controls.prepare_background_restart = runtime.background_schedule.prepare_restart
    if controls is not None:
        controls.bind_cleanup_status(runtime.cleanup_status)
        controls.bind_admission_status(runtime.admission_status)
        runtime.restart_requested = controls.restart_ready
    return runtime
