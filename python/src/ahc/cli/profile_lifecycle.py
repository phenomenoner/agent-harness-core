"""Explicit per-agent supervisor ownership, separate from the base app owner."""

from dataclasses import replace

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json


def profile_config(config, agent):
    if config.profile_registry is None or agent not in dict(config.foreground_configs):
        raise ContractError("profile lifecycle requires a declared foreground")
    config.profile_registry.profile(agent)
    if config.services:
        raise ContractError("profile foreground requires a service-free app configuration")
    return replace(config, state_dir=config.state_dir / "profiles" / digest_json({"agent": agent}))


def control_config(args, config):
    agent = getattr(args, "agent", None)
    return (profile_config(config, agent)
            if args.command in ("status", "stop") and agent is not None else config)


def selected_supervisor(args, config):
    from ahc.application.lifecycle import Supervisor

    from .foreground_selection import compose_selected_foreground

    if (config.services and config.foreground_configs
            and (args.agent is not None or args.runtime_config is not None)):
        raise ContractError("profile foreground requires a service-free app configuration")
    runtime = compose_selected_foreground(args, config)
    if runtime is not None and config.foreground_configs:
        config = profile_config(config, runtime.session.agent_id)
    return runtime, Supervisor(config) if runtime is None else Supervisor(config, runtime=runtime)
