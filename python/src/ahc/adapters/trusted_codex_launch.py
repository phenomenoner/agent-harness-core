"""Explicit trusted-user launch; verify pinned bytes without copying credentials."""

import base64
import json
import posixpath
import zlib
from importlib.resources import files

from ahc.domain.sessions import ContractError


def trusted_codex_command(*, bundle, codex_home, cwd, interpreter, approval_policy,
                         sealed_history=False, staged_authentication=False, user_environment=None,
                         sqlite_home=None, tool_free=False):
    if type(tool_free) is not bool:
        raise ContractError("tool-free launch flag invalid")
    if tool_free:
        require_tool_free_configuration(codex_home, cwd)
    paths = (bundle, codex_home, cwd, interpreter)
    for path in paths + (() if sqlite_home is None else (sqlite_home,)):
        if (
            type(path) is not str
            or not path.startswith("/")
            or path.startswith("//")
            or posixpath.normpath(path) != path
            or len(path) > 4096
            or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in path)
        ):
            raise ContractError("trusted Codex paths must be explicit normalized POSIX paths")
    if approval_policy not in ("on-request", "never"):
        raise ContractError("trusted Codex approval policy unsupported")
    if type(sealed_history) is not bool:
        raise ContractError("trusted Codex history mode invalid")
    if (type(staged_authentication) is not bool
            or staged_authentication and not sealed_history):
        raise ContractError("trusted Codex staged authentication requires sealed history")
    if user_environment is not None and (
        sealed_history or staged_authentication or type(user_environment) is not dict
        or not {"HOME", "PATH"} <= set(user_environment)
        or set(user_environment) - {"HOME", "PATH", "USER", "LOGNAME", "WSL_INTEROP"}
        or any(type(value) is not str or "\0" in value for value in user_environment.values())
    ):
        raise ContractError("trusted Codex full-user environment is invalid or mixed with isolation")
    resources = files("ahc.adapters")
    payload = {
        "verifier": resources.joinpath("linux_codex_tree.py").read_text(encoding="utf-8"),
        "pin": json.loads(resources.joinpath("codex_linux_backend.json").read_text()),
        "bundle": bundle,
        "home": codex_home,
        "cwd": cwd,
        "approval": approval_policy,
        "sealed": sealed_history,
        "authentication": staged_authentication,
        "overrides": {},
        "user_environment": user_environment,
    }
    if user_environment is not None:
        payload["overrides"] = {
            "projects": {cwd: {"trust_level": "trusted"}},
            "sandbox_mode": "danger-full-access",
        }
    if staged_authentication:
        payload["overrides"] = {
            "cli_auth_credentials_store": "file", "web_search": "disabled",
            "features.skip_host_skill_discovery": True,
            # The caller already granted this exact protected work directory.
            # Supply trust in memory; sealed HOME cannot persist config.toml.
            "projects": {cwd: {"trust_level": "trusted"}},
            "suppress_unstable_features_warning": True,
            # The pinned model may require CodeModeOnly independently of the
            # feature flag. Keep the admitted AHC bridge directly callable.
            "features.code_mode": {
                "enabled": False, "direct_only_tool_namespaces": ["workspace"]},
        }
    if tool_free:
        payload["overrides"].update({
            "web_search": "disabled", "mcp_servers": {},
            "features.skip_host_skill_discovery": True,
            "features.code_mode": {"enabled": False, "direct_only_tool_namespaces": []},
            "suppress_unstable_features_warning": True,
        })
    if staged_authentication or tool_free:
        for key in (
            "shell_tool", "unified_exec", "apps", "multi_agent", "multi_agent_v2",
            "browser_use", "browser_use_external", "browser_use_full_cdp_access", "in_app_browser",
            "skill_mcp_dependency_install", "skill_search", "code_mode_host", "hooks",
            "plugins", "computer_use", "image_generation", "workspace_dependencies",
            "shell_snapshot", "apply_patch_freeform", "goals",
        ):
            payload["overrides"]["features." + key] = False
    if sqlite_home is not None:
        payload["overrides"]["sqlite_home"] = sqlite_home
    script = (
        "import types,sys,os,pathlib\n"
        "m=types.ModuleType('ahc_trusted_tree');sys.modules[m.__name__]=m\n"
        "exec(compile(p['verifier'],'linux_codex_tree.py','exec'),m.__dict__)\n"
        "try:\n"
        " assert os.getuid()!=0\n"
        " m.verify_linux_tree(pathlib.Path(p['bundle']),pin=p['pin'],"
        "target=p['pin']['target'],require_root_owned=False)\n"
        " assert pathlib.Path(p['home']).is_dir() and pathlib.Path(p['cwd']).is_dir()\n"
        " os.chdir(p['cwd'])\n"
        " exe=str(pathlib.Path(p['bundle'])/p['pin']['entrypoint'])\n"
        " env={'PATH':'/usr/bin:/bin','LANG':'C.UTF-8','HOME':p['home'],'CODEX_HOME':p['home']}\n"
        " if p['user_environment'] is not None: env.update(p['user_environment'])\n"
        " args=[exe,'app-server','--strict-config','-c',"
        "'approval_policy='+json.dumps(p['approval'])]\n"
        " if p['sealed']:\n"
        "  args+=['-c','sqlite_home='+json.dumps(p['home']+'/state')]\n"
        " for key,value in p['overrides'].items():\n"
        "  literal=('{'+json.dumps(p['cwd'])+'={trust_level=\"trusted\"}}'"
        " if key=='projects' else '{enabled=false,direct_only_tool_namespaces=[\"workspace\"]}'"
        " if key=='features.code_mode' else json.dumps(value))\n"
        "  args+=['-c',key+'='+literal]\n"
        " os.execve(exe,args,env)\n"
        "except BaseException:\n"
        " sys.stderr.write('trusted Codex launch unavailable\\n');sys.exit(78)\n"
    )
    payload["launcher"] = script
    encoded = base64.b85encode(zlib.compress(
        json.dumps(payload, separators=(",", ":")).encode(), level=9)).decode()
    argument = (
        "import base64,json,zlib;p=json.loads(zlib.decompress(base64.b85decode("
        + repr(encoded) + ")));exec(compile(p['launcher'],'ahc_trusted_launch','exec'))"
    )
    if len(argument) <= 4096:
        return (interpreter, "-I", "-u", "-c", argument)
    # Larger official resource manifests keep the same verifier, complete pin
    # and policy. Carry the compressed payload as bounded data arguments rather
    # than expanding the existing per-argument limit or introducing temp files.
    decoder = (
        "import base64,json,zlib,sys;p=json.loads(zlib.decompress("
        "base64.b85decode(''.join(sys.argv[1:]))));"
        "exec(compile(p['launcher'],'ahc_trusted_launch','exec'))"
    )
    chunks = tuple(encoded[i:i + 4096] for i in range(0, len(encoded), 4096))
    if len(chunks) > 8:
        raise ContractError("trusted Codex launch exceeds existing unit command bound")
    return (interpreter, "-I", "-u", "-c", decoder, *chunks)

def require_tool_free_configuration(codex_home, cwd):
    """Reject configured external MCP surfaces that feature switches cannot revoke."""
    from pathlib import Path
    import tomllib
    paths = [Path(codex_home) / "config.toml"]
    directory = Path(cwd)
    paths.extend(p / ".codex/config.toml" for p in (directory, *directory.parents))
    for path in dict.fromkeys(paths):
        if path.is_file():
            raw = path.read_bytes()
            if len(raw) > 1048576:
                raise ContractError("tool-free host configuration exceeds bound")
            document = tomllib.loads(raw.decode("utf-8"))
            if document.get("mcp_servers"):
                raise ContractError("tool-free host decisions do not admit configured MCP servers")
