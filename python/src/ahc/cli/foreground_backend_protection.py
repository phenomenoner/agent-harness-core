"""Explicit native backend profile; unsupported effect routes refuse admission."""

from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.sessions import ContractError

from .foreground_workspace_protection import parse_native_protection

_EXTRA = {"network_mode", "home_mode", "home_bytes", "temporary_bytes"}


def parse_backend_protection(value):
    if type(value) is not dict or not _EXTRA <= set(value):
        raise ContractError("backend.protection fields invalid")
    if value["network_mode"] != "shared" or value["home_mode"] != "sealed-history":
        raise ContractError("backend.protection requires shared network and sealed history")
    NativeScratchLimits(value["home_bytes"], value["temporary_bytes"])
    result = parse_native_protection({k: v for k, v in value.items() if k not in _EXTRA}, None)
    return result | {key: value[key] for key in _EXTRA}


def require_protected_routes(document):
    from .foreground_user_access import USER_FULL_ACCESS, require_user_full_access

    user_access = document.get("execution_profile") == USER_FULL_ACCESS
    if user_access:
        require_user_full_access(document)
    if "protection" not in document["backend"]:
        return
    workspace = document["workspace"]
    if workspace["kind"] not in ("linux-python", "linux-ipython") or (
            not user_access and "protection" not in workspace):
        raise ContractError("protected backend requires a protected native Python workspace")
    if (not user_access and document["backend"].get("authentication") is not None
            and document["backend"]["codex_home"]
            not in workspace["protection"]["protected_roots"]):
        raise ContractError("staged backend authentication home must be hidden from workspace")
    required = [document["runtime_root"]]
    if document["skill_root"] is not None:
        required.append(document["skill_root"])
    protected = [document["backend"]["protection"]]
    if not user_access:
        protected.append(workspace["protection"])
    for profile in protected:
        if not set(required) <= set(profile["protected_roots"]):
            raise ContractError("protected routes must explicitly hide runtime and skill stores")
    profiles = [p for p in document.get("mcp", {}).get("profiles", []) if p["enabled"]]
    native = document.get("native_minion", {}).get("profile", {})
    if native.get("enabled"):
        profiles.append(native)
    for profile in profiles:
        if (profile.get("schema_version") != "ahc.mcp-stdio-profile.v1"
                or profile.get("cwd") != workspace["cwd"]
                or profile.get("environment_names") != []):
            raise ContractError(
                "protected backend does not admit an unqualified MCP/minion profile")
    if profiles and document.get("mcp", {}).get("credential_environment"):
        raise ContractError("protected stdio routes do not import MCP credentials")


def native_backend_protection(config, *, provider_auth=False):
    value = config.backend.get("protection")
    if value is None:
        return {}
    from ahc.infrastructure.process.linux_backend_boundary import LinuxBackendBoundary

    return {
        "boundary": LinuxBackendBoundary(
            executable=value["executable"],
            sha256=value["sha256"],
            workspace=config.workspace["cwd"],
            readonly_roots=value["readonly_roots"],
            protected_roots=value["protected_roots"],
            state_root=config.backend["codex_home"],
            home_bytes=value["home_bytes"],
            temporary_bytes=value["temporary_bytes"],
            staged_authentication=(
                provider_auth and config.backend.get("authentication") == "staged-codex-file"
            ),
        ),
        "resource_limits": NativeResourceLimits(**value["resource_limits"]),
    }
