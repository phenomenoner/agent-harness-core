"""Select and retain foreground bytes without runtime construction or discovery."""

from ahc.domain.sessions import ContractError

from .foreground import read_foreground_config
from .foreground_config_native import parse_trusted_foreground_config


def select_foreground(args, app):
    agent, path = args.agent, args.runtime_config
    if agent is not None and path is not None:
        raise ContractError("foreground selectors conflict")
    paths = dict(app.foreground_configs)
    if agent is not None:
        if app.profile_registry is None or agent not in paths:
            raise ContractError("foreground agent has no declared configuration")
        app.profile_registry.profile(agent)
        document = read_foreground_config(paths[agent])
        config = parse_trusted_foreground_config(document)
        if config.session.agent_id != agent:
            raise ContractError("selected foreground agent mismatch")
        return config.to_dict()
    if path is None:
        return None
    document = read_foreground_config(path)
    if paths:
        config = parse_trusted_foreground_config(document)
        selected = paths.get(config.session.agent_id)
        if selected is None:
            raise ContractError("foreground agent has no declared configuration")
        # Reuse the retained document when the same path is explicitly selected.
        if path.absolute() != selected.absolute():
            expected = parse_trusted_foreground_config(read_foreground_config(selected))
            if config.to_dict() != expected.to_dict():
                raise ContractError("foreground differs from app-owned selection")
        return config.to_dict()
    return document


def compose_selected_foreground(args, app):
    from .foreground import compose_foreground_document

    document = select_foreground(args, app)
    return (None if document is None else
            compose_foreground_document(document, profile_registry=app.profile_registry))
