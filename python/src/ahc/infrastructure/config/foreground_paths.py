"""Explicit registered-agent to relative foreground file declarations."""

from pathlib import PureWindowsPath

from ahc.domain.sessions import ContractError


def foreground_paths(value, registry, base):
    if value is None:
        return ()
    if type(value) is not dict or not value or len(value) > 256 or registry is None:
        raise ContractError("foreground mapping invalid")
    result = []
    for agent, path in value.items():
        registry.profile(agent)
        if (type(path) is not str or not path or any(ord(c) < 32 for c in path)
                or PureWindowsPath(path).is_absolute() or PureWindowsPath(path).drive
                or path.startswith(("/", "\\")) or ".." in PureWindowsPath(path).parts):
            raise ContractError("foreground path must be explicit and relative")
        result.append((agent, base / path))
    return tuple(sorted(result))
