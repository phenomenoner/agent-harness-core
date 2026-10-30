"""Stdlib protected birth metadata storage for the Linux unit controller."""

import json
import os
import re
import stat
from uuid import UUID


class ControlError(RuntimeError):
    pass


def birth_identity(value):
    identity = {key: value[key] for key in ("distribution", "boot_id", "unit", "invocation_id")}
    if (
        any(type(item) is not str for item in identity.values())
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", identity["distribution"])
        or str(UUID(identity["boot_id"])) != identity["boot_id"]
        or not re.fullmatch(r"ahc-backend-[0-9a-f]{32}\.service", identity["unit"])
        or type(identity["invocation_id"]) is not str
        or not re.fullmatch(r"[0-9a-f]{32}", identity["invocation_id"])
        or identity["invocation_id"] == "0" * 32
    ):
        raise ControlError("invalid birth identity")
    return identity


def save_birth(root, unit, value, *, sync):
    identity = birth_identity(value)
    if identity["unit"] != unit or value["state"] != "running":
        raise ControlError("birth identity not running")
    descriptor = os.open(
        root / (unit + ".identity"), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "w") as output:
        json.dump(identity, output)
        output.flush()
        os.fsync(output.fileno())
    sync()


def sync_root(root):
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def read_birth(root, unit, *, object_hook):
    descriptor = os.open(root / (unit + ".identity"), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != 0
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_size > 4096
        ):
            raise ControlError("birth record unprotected")
        value = json.loads(os.read(descriptor, 4097), object_pairs_hook=object_hook)
        if type(value) is not dict or set(value) != {
            "distribution",
            "boot_id",
            "unit",
            "invocation_id",
        }:
            raise ControlError("birth record malformed")
        return birth_identity(value)
    finally:
        os.close(descriptor)
