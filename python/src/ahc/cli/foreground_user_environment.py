"""Ordinary-user workspace environment; Windows interop remains optional."""

import os
import stat
from pathlib import Path

from .foreground_user_access import USER_FULL_ACCESS, require_user_full_access


def _user_environment():
    import pwd

    account = pwd.getpwuid(os.geteuid())
    return {"HOME": account.pw_dir, "USER": account.pw_name,
            "LOGNAME": account.pw_name, "PATH": "/usr/local/bin:/usr/bin:/bin"}


def _interop_socket():
    # Use the guest's long-lived init server, never a guessed PID or a socket
    # borrowed from an operator shell. Discovery is local and launches nothing.
    try:
        candidates = sorted(Path("/run/WSL").glob("[0-9]*_interop"))
        if len(candidates) > 64:
            return None
        for path in candidates:
            pid = path.name.removesuffix("_interop")
            if not pid.isdecimal():
                continue
            try:
                fields = dict(line.split(":", 1) for line in
                              (Path("/proc") / pid / "status").read_text().splitlines()
                              if ":" in line)
                name = fields.get("Name", "").strip()
                init = (name in {"init", "init-systemd"}
                        or name.startswith("init(") or name.startswith("init-systemd("))
                # /proc/<root-pid>/exe may require ptrace permission. Public
                # status fields identify WSL init without granting that access.
                info = path.lstat()
                if (stat.S_ISSOCK(info.st_mode) and info.st_uid == 0 and init
                        and fields.get("Uid", "").split() == ["0"] * 4):
                    return str(path)
            except OSError:
                continue
    except OSError:
        pass
    return None


def native_workspace_environment(document):
    if document.get("execution_profile") != USER_FULL_ACCESS:
        return {}  # Existing protected and omitted profiles keep their contract.
    require_user_full_access(document)
    environment = _user_environment()
    for row in document.get("host_paths", ()):
        if row["host_root"] == "C:/":
            root = row["backend_root"]
            environment["PATH"] += (f":{root}/Windows/System32"
                                    f":{root}/Windows/System32/WindowsPowerShell/v1.0")
    socket = _interop_socket()
    if socket is not None:
        environment["WSL_INTEROP"] = socket
    return environment
