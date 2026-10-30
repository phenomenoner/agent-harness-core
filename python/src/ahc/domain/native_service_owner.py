"""Admit only declared systemd owner layouts, without filesystem effects."""

from pathlib import PurePosixPath

CONTROLLER_SUBGROUP = "ahc-controller"


def service_path(relative: str, *, setup: bool = False) -> PurePosixPath:
    path = PurePosixPath(relative)
    if (not relative.startswith("/") or relative.startswith("//")
            or str(path) != relative or ".." in path.parts):
        raise ValueError("invalid owner cgroup path")
    if setup:
        if path.name != ".control":
            raise ValueError("resource setup requires the systemd control subgroup")
        path = path.parent
    elif path.name == CONTROLLER_SUBGROUP:
        path = path.parent
    if not path.name.endswith(".service") or path.name.startswith("-"):
        raise ValueError("owner requires a service or its declared controller subgroup")
    return path


def validate_owner(relative: str, invocation: str, fields: dict[str, str],
                   *, setup: bool = False) -> PurePosixPath:
    service = service_path(relative, setup=setup)
    if len(invocation) != 32 or any(c not in "0123456789abcdef" for c in invocation):
        raise ValueError("invalid service invocation")
    expected = {"ControlGroup": str(service), "InvocationID": invocation,
                "Delegate": "yes", "KillMode": "control-group", "SendSIGKILL": "yes"}
    declared = fields.get("DelegateSubgroup", "")
    actual = {key: value for key, value in fields.items() if key != "DelegateSubgroup"}
    partitioned = setup or str(service) != relative
    if actual != expected or declared != (CONTROLLER_SUBGROUP if partitioned else ""):
        raise ValueError("service owner properties or subgroup do not match")
    return service
