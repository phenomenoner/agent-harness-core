"""Linux-only, stdlib root control helper for transient ordinary-user units.

All AHC creators/stoppers must use this guard. An independent root administrator
remains outside this ownership boundary. This is not executable policy admission.
"""

import contextlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from uuid import UUID

# A WSL distribution can lose /run without changing the shared VM boot ID.
# Keep reservations across distribution restarts; UUID names are never reused.
ROOT = Path("/var/lib/ahc-backend-custody")


if globals().get("__package__"):
    from . import linux_unit_metadata as metadata
else:
    import linux_unit_metadata as metadata

ControlError = metadata.ControlError
_birth_identity = metadata.birth_identity


def _save_birth(unit, value):
    return metadata.save_birth(ROOT, unit, value, sync=_sync_root)


def _sync_root():
    return metadata.sync_root(ROOT)


def _read_birth(unit):
    return metadata.read_birth(ROOT, unit, object_hook=_object)


def _run(arguments):
    result = subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        timeout=20,
        env={"PATH": "/usr/bin:/bin", "LANG": "C"},
    )
    if result.returncode:
        raise ControlError("system manager operation failed")
    return result.stdout


@contextlib.contextmanager
def _guard(unit):
    if sys.platform != "linux":
        raise ControlError("Linux controller required")
    import fcntl

    if os.geteuid() != 0:
        raise ControlError("root controller required")
    try:
        ROOT.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = ROOT.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o700:
        raise ControlError("control directory ownership invalid")
    descriptor = os.open(ROOT / (unit + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != 0
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ControlError("control lock ownership invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ControlError("unit controller busy") from None
        yield
    finally:
        os.close(descriptor)


def _observe(unit):
    raw = _run(
        [
            "/usr/bin/systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,InvocationID,ControlGroup,Restart,User,KillMode",
        ]
    )
    fields = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in fields:
            raise ControlError("invalid unit observation")
        fields[key] = value
    required = {
        "LoadState",
        "ActiveState",
        "InvocationID",
        "ControlGroup",
        "Restart",
        "User",
        "KillMode",
    }
    if set(fields) != required:
        raise ControlError("incomplete unit observation")
    if fields["LoadState"] == "not-found":
        state = "absent"
        invocation = None
    else:
        if (
            fields["Restart"] != "no"
            or fields["User"] != "ahc"
            or fields["KillMode"] != "control-group"
        ):
            raise ControlError("unit policy drift")
        state = "stopped" if fields["ActiveState"] in ("inactive", "failed") else "running"
        invocation = fields["InvocationID"]
    expected_group = "/system.slice/" + unit
    if fields["ControlGroup"] not in ("", expected_group):
        raise ControlError("unit cgroup mismatch")
    group = Path("/sys/fs/cgroup" + expected_group)
    if group.exists():
        events = dict(line.split() for line in (group / "cgroup.events").read_text().splitlines())
        empty = events.get("populated") == "0"
    else:
        empty = True
    return {
        "distribution": os.environ.get("WSL_DISTRO_NAME"),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "unit": unit,
        "invocation_id": invocation,
        "state": state,
        "cgroup_empty": empty,
    }


def _reconcile_creation(unit, history):
    # Caller holds the SAME guard as create. Reserving .created also fences
    # delayed controllers running older helper code, which use O_EXCL here.
    before = _observe(unit)
    if not history.exists() and not history.is_symlink():
        if (before["state"] != "absent" or before["invocation_id"] is not None
                or before["cgroup_empty"] is not True):
            raise ControlError("unreserved unit presence unverified")
        fd = os.open(history, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as output:
            json.dump({"unit": unit, "cancelled": True}, output)
            output.flush()
            os.fsync(output.fileno())
        _sync_root()
    descriptor = os.open(history, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 4096):
            raise ControlError("creation reservation unprotected")
        reservation = json.loads(os.read(descriptor, 4097), object_pairs_hook=_object)
        # A previous reply may have been lost during fsync. Re-establish file
        # durability before returning the cancellation fence on a later call.
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if (type(reservation) is dict and set(reservation) == {"unit", "cancelled"}
            and reservation["unit"] == unit and reservation["cancelled"] is True):
        if (before["state"] != "absent" or before["invocation_id"] is not None
                or before["cgroup_empty"] is not True):
            raise ControlError("cancelled attempt unexpectedly present")
        _sync_root()
        return {"unit": unit, "cancelled": True, "identity": None}
    # A reserved name without complete protected birth evidence is UNKNOWN,
    # even if systemd currently reports absent: a launch may still be in flight.
    identity = _read_birth(unit)
    if (type(reservation) is not dict
            or reservation != {"unit": unit, "boot_id": identity["boot_id"]}
            or identity["unit"] != unit or identity["distribution"] != before["distribution"]):
        raise ControlError("creation birth binding unverified")
    return {"unit": unit, "cancelled": False, "identity": identity}


def control(request, *, launch=None):
    if type(request) is not dict:
        raise ControlError("invalid request")
    unit = request.get("unit")
    action = request.get("action")
    if type(unit) is not str or re.fullmatch(r"ahc-backend-[0-9a-f]{32}\.service", unit) is None:
        raise ControlError("unit outside owned namespace")
    keys = {"action", "unit"}
    if action == "create":
        keys |= {"command", "lifetime_seconds"}
    elif action in ("stop", "retire"):
        keys |= {"boot_id", "invocation_id"}
    elif action not in ("observe", "reconcile-create"):
        raise ControlError("unsupported action")
    if set(request) != keys:
        raise ControlError("request fields invalid")
    with _guard(unit):
        history = ROOT / (unit + ".created")
        if action == "reconcile-create":
            return _reconcile_creation(unit, history)
        if action == "create":
            command, lifetime = request["command"], request["lifetime_seconds"]
            if (
                type(command) is not list
                or not 1 <= len(command) <= 64
                or any(
                    type(arg) is not str
                    or not arg
                    or len(arg) > 4096
                    or any(ord(c) < 32 for c in arg)
                    for arg in command
                )
                or not command[0].startswith("/")
                or type(lifetime) is not int
                or not 1 <= lifetime <= 3600
            ):
                raise ControlError("invalid workload command or lifetime")
            before = _observe(unit)
            if before["state"] != "absent" or not before["cgroup_empty"]:
                raise ControlError("unit already exists")
            # A reserved name is never reused, including restart and response loss.
            fd = os.open(history, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w") as output:
                json.dump({"unit": unit, "boot_id": before["boot_id"]}, output)
                output.flush()
                os.fsync(output.fileno())
            _sync_root()
            arguments = [
                "/usr/bin/systemd-run",
                "--expand-environment=no",
                "--unit=" + unit,
                "--uid=ahc",
                "--property=Restart=no",
                "--property=KillMode=control-group",
                "--property=NoNewPrivileges=yes",
                "--property=TimeoutStopSec=5",
                "--property=RuntimeMaxSec=" + str(lifetime),
                "--",
                *command,
            ]
            if launch is not None:
                value = launch(arguments, unit)
            else:
                _run(arguments)
                value = _observe(unit)
            _save_birth(unit, value)
            return value
        if not history.is_file() or history.is_symlink():
            raise ControlError("unit creation provenance unavailable")
        before = _observe(unit)
        if action == "retire":
            identity = _read_birth(unit)
            if (
                identity
                != {
                    "distribution": before["distribution"],
                    "unit": unit,
                    "boot_id": request["boot_id"],
                    "invocation_id": request["invocation_id"],
                }
                or before["boot_id"] == identity["boot_id"]
                or str(UUID(before["boot_id"])) != before["boot_id"]
                or before["state"] != "absent"
                or before["invocation_id"] is not None
                or before["cgroup_empty"] is not True
            ):
                raise ControlError("retirement unverified")
            # Read-only proof: never stop a different-boot unit or rewrite birth.
            return {"identity": identity, "current_boot_id": before["boot_id"]}
        if action == "stop":
            if (
                before["boot_id"] != request["boot_id"]
                or before["invocation_id"] != request["invocation_id"]
                or not request["invocation_id"]
            ):
                raise ControlError("unit identity changed")
            _run(["/usr/bin/systemctl", "stop", unit])
            after = _observe(unit)
            if after["state"] not in ("stopped", "absent") or not after["cgroup_empty"]:
                raise ControlError("unit cleanup unverified")
            return after
        return before


def main():
    try:
        raw = sys.stdin.buffer.readline(65537)
        if len(raw) > 65536 or not raw.endswith(b"\n"):
            raise ControlError("request frame invalid")
        result = {"ok": True, "result": control(json.loads(raw, object_pairs_hook=_object))}
    except (ControlError, ValueError, OSError, subprocess.SubprocessError):
        result = {"ok": False, "error": "unit control unverified"}
    print(json.dumps(result), flush=True)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ControlError("duplicate request field")
        result[key] = value
    return result


if __name__ == "__main__":
    main()
