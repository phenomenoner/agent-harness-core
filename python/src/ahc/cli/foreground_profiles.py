"""Bind a foreground agent to an explicitly configured application registry."""

from dataclasses import replace

from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import digest_json


def require_profile_agent(document, registry):
    if registry is None:
        return
    if type(registry) is not AgentProfileRegistry or type(document) is not dict:
        raise ContractError("foreground profile authority invalid")
    session = VirtualSessionIdentity.from_dict(document.get("session"))
    if not any(profile.agent_id == session.agent_id for profile in registry.profiles):
        raise ContractError("foreground agent is not in the configured profile registry")


def bind_profile_configuration(config, registry):
    require_profile_agent(config.to_dict(), registry)
    from .foreground_agent_context import bind_agent_context

    bind_agent_context(config, registry)
    if registry is None:
        return config
    return replace(config, configuration_digest=digest_json({
        "foreground_configuration": config.configuration_digest,
        "profile_registry": registry.to_dict(),
        "agent_id": config.session.agent_id,
    }))
