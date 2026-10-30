"""Render an explicit foreground owner service; rendering never starts a receiver."""

import re
from pathlib import PurePosixPath

from ahc.domain.native_service_owner import CONTROLLER_SUBGROUP
from ahc.domain.sessions import ContractError


def _path(value):
    if (type(value) is not str or not re.fullmatch(r"/[A-Za-z0-9._/-]+", value)
            or value == "/" or str(PurePosixPath(value)) != value
            or ".." in PurePosixPath(value).parts
            or value == "/mnt" or value.startswith("/mnt/")):
        raise ContractError("service paths require normalized absolute portable POSIX names")
    return value


def render_user_service(*, interpreter, config, agent, workspace, credential_file,
                        resource_partition=False):
    """One owner per selected config/profile; separate channel stores need distinct units."""
    if type(agent) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", agent):
        raise ContractError("service agent identifier invalid")
    interpreter, config, workspace, credential_file = map(
        _path, (interpreter, config, workspace, credential_file))
    if type(resource_partition) is not bool:
        raise ContractError("resource partition selection must be a boolean")
    partition = ("Slice=-.slice\nDelegateSubgroup=" + CONTROLLER_SUBGROUP + "\n"
                 "ExecStartPre=" + interpreter + " -I -m ahc.cli.linux_resource_owner\n"
                 if resource_partition else "")
    return (
        "[Unit]\nDescription=AHC native agent " + agent + "\n"
        "StartLimitIntervalSec=60\nStartLimitBurst=3\n\n"
        "[Service]\nType=exec\n"
        "ExecStart=" + interpreter + " -I -m ahc run --config " + config
        + " --agent " + agent + "\n"
        "WorkingDirectory=" + workspace + "\n"
        "Environment=PATH=/usr/local/bin:/usr/bin:/bin\nEnvironment=LANG=C.UTF-8\n"
        "EnvironmentFile=" + credential_file + "\n"
        "UnsetEnvironment=WSL_INTEROP WSLENV\n"
        "UMask=0077\nDelegate=yes\nKillMode=control-group\nSendSIGKILL=yes\n"
        + partition +
        "TimeoutStopSec=30\nRestart=on-failure\nRestartSec=3\n"
        "StandardOutput=journal\nStandardError=journal\n\n"
        "[Install]\nWantedBy=default.target\n"
    )
