"""An explicit ordinary-user grant for daily native workspace execution."""
from ahc.domain.sessions import ContractError

USER_FULL_ACCESS = "user-full-access"


def parse_user_access(raw, *, native):
    if "execution_profile" not in raw:
        return {}
    if not native:
        raise ContractError("execution_profile requires native Linux configuration")
    require_user_full_access(raw)
    return {"execution_profile": USER_FULL_ACCESS}


def require_user_full_access(document):
    workspace = document.get("workspace")
    policy = document.get("policy")
    if (document.get("schema_version") != "ahc.foreground.config.v3"
            or document.get("execution_profile") != USER_FULL_ACCESS
            or type(workspace) is not dict
            or workspace.get("kind") not in ("linux-python", "linux-ipython")
            or "protection" in workspace
            or type(policy) is not dict
            or policy.get("sandbox_mode") != "danger-full-access"
            or policy.get("workspace_granted") is not True):
        raise ContractError("user-full-access requires an explicit full-access native workspace grant")
    # This authorizes existing OS-user access; it does not change UID, sudo,
    # mount Windows drives, activate another agent or broaden a tool contract.
    return True
