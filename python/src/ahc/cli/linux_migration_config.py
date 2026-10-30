"""Explicit path mapping for a reviewed Windows/WSL to Linux migration."""

from ahc.domain.sessions import ContractError
from .foreground_config_native import parse_foreground_config_native
from .foreground_config_v2 import parse_foreground_config_v2


def convert_foreground_to_linux(document, *, runtime_root, cwd, interpreter,
                                bundle, codex_home, skill_root, native_memory=None,
                                memory=None, operating_context=None):
    """Preserve routes, grants and preferences; bind the new environment explicitly.

    Optional MCP and native-minion launch contracts require their own migration;
    refusing them avoids silently retaining a Windows execution dependency.
    """
    result = parse_foreground_config_v2(document).to_dict()
    if result.get("mcp") or result.get("native_minion"):
        raise ContractError("optional execution bindings need explicit migration")
    for name, replacement in (("native_memory", native_memory), ("memory", memory),
                               ("operating_context", operating_context)):
        if name in result:
            if replacement is None:
                raise ContractError(name + " requires explicit migration mapping")
            result[name] = replacement
        elif replacement is not None:
            raise ContractError(name + " cannot be added by relocation")
    result["schema_version"] = "ahc.foreground.config.v3"
    result["runtime_root"] = runtime_root
    result["skill_root"] = skill_root
    workspace = result["workspace"]
    workspace["kind"] = {"wsl-python": "linux-python", "wsl-ipython": "linux-ipython"}[workspace["kind"]]
    for key in ("wsl_executable", "distribution", "user"):
        del workspace[key]
    workspace.update(cwd=cwd, interpreter=interpreter)
    result["backend"].update(kind="codex-linux-trusted", bundle=bundle, codex_home=codex_home)
    result["policy"]["revision"] += 1
    return parse_foreground_config_native(result).to_dict()
