"""Explicit no-new-resource unavailability, never a generic startup error wrapper."""

from pathlib import Path

from ahc.domain.sessions import ContractError


class WorkspacePreflightUnavailable(ContractError):
    """Raised only before resource creation; existing retained custody is unchanged."""

    reason_code = "host-launcher-unavailable"


def require_workspace_launcher(executable):
    if not Path(executable).is_file():
        raise WorkspacePreflightUnavailable("workspace host launcher unavailable")


def inspect_workspace_launcher(executable):
    scope = {"observation_scope": "host-launcher-file-presence", "execution_verified": False}
    try:
        require_workspace_launcher(executable)
    except WorkspacePreflightUnavailable:
        return {**scope, "status": "unavailable",
                "reason_code": WorkspacePreflightUnavailable.reason_code}
    except OSError:
        return {**scope, "status": "indeterminate", "reason_code": "host-inspection-failed"}
    return {**scope, "status": "present-unverified", "reason_code": None}
