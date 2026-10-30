"""Stable exports loaded only when the corresponding capability is selected."""

_EXPORTS = {
    "AgentBackend": ".agent_backend_port",
    "CREDENTIAL_DOCTOR_SCHEMA_VERSION": ".credential_doctor",
    "CredentialUseFacade": ".credential_facade",
    "CredentialUseResult": ".credential_facade",
    "INVALID_STATE": ".lifecycle",
    "JudgmentService": ".judgment_service",
    "LEASE_BUSY": ".lifecycle",
    "NATIVE_PLUGIN_DOCTOR_SCHEMA_VERSION": ".plugin_doctor",
    "NOT_RUNNING": ".lifecycle",
    "NativePluginHost": ".plugin_host",
    "OK": ".lifecycle",
    "STOP_SUPERSEDED": ".lifecycle",
    "STOP_TIMEOUT": ".lifecycle",
    "SUPERVISOR_FAILURE": ".lifecycle",
    "SkillTurnService": ".skill_service",
    "Supervisor": ".lifecycle",
    "WorkingContinuityService": ".working_continuity",
    "WorkingSetPort": ".working_continuity",
    "execute_fake_turn": ".fake_turn",
    "inspect_credential_facade": ".credential_doctor",
    "inspect_native_plugin_host": ".plugin_doctor",
    "inspect_status": ".lifecycle",
    "learn_episode": ".skill_learning",
    "reflect_lesson": ".skill_learning",
    "request_stop": ".lifecycle",
}

__all__ = [
    "AgentBackend",
    "CREDENTIAL_DOCTOR_SCHEMA_VERSION",
    "CredentialUseFacade",
    "CredentialUseResult",
    "INVALID_STATE",
    "JudgmentService",
    "LEASE_BUSY",
    "NATIVE_PLUGIN_DOCTOR_SCHEMA_VERSION",
    "NOT_RUNNING",
    "NativePluginHost",
    "OK",
    "STOP_SUPERSEDED",
    "STOP_TIMEOUT",
    "SUPERVISOR_FAILURE",
    "SkillTurnService",
    "Supervisor",
    "WorkingContinuityService",
    "WorkingSetPort",
    "execute_fake_turn",
    "inspect_credential_facade",
    "inspect_native_plugin_host",
    "inspect_status",
    "learn_episode",
    "reflect_lesson",
    "request_stop",
]


def _load_module(relative: str):
    # Static import targets keep optional loading auditable and capability-local.
    if relative == ".agent_backend_port":
        from . import agent_backend_port as selected
    elif relative == ".credential_doctor":
        from . import credential_doctor as selected
    elif relative == ".credential_facade":
        from . import credential_facade as selected
    elif relative == ".lifecycle":
        from . import lifecycle as selected
    elif relative == ".judgment_service":
        from . import judgment_service as selected
    elif relative == ".plugin_doctor":
        from . import plugin_doctor as selected
    elif relative == ".plugin_host":
        from . import plugin_host as selected
    elif relative == ".skill_service":
        from . import skill_service as selected
    elif relative == ".working_continuity":
        from . import working_continuity as selected
    elif relative == ".fake_turn":
        from . import fake_turn as selected
    elif relative == ".skill_learning":
        from . import skill_learning as selected
    else:
        raise AttributeError("unsupported application export module")
    return selected


def __getattr__(name: str):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(_load_module(module), name)
    globals()[name] = value
    return value
