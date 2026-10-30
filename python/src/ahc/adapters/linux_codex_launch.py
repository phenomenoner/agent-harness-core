"""Prepare an offline Codex unit from protected Linux artifacts; never spawn.

The caller supplies the pinned bundle verifier and owns creation serialization.
Privileged root mutation and full application admission are outside this check.
"""

import hashlib
import os
import posixpath
import re
import stat
import sys
import tomllib
from pathlib import Path


class LaunchRejected(ValueError):
    pass


def _path(value):
    # Restrict systemd path-list metacharacters; no quoting/expansion is inferred.
    if (
        type(value) is not str
        or not re.fullmatch(r"/[A-Za-z0-9_./-]+", value)
        or value == "/"
        or value.startswith("//")
        or posixpath.normpath(value) != value
    ):
        raise LaunchRejected("unsupported launch path")
    return Path(value)


def _protected(path, *, directory):
    for current in (path, *path.parents):
        info = current.lstat()
        expected_dir = directory or current != path
        if (
            info.st_uid != 0
            or (expected_dir and not stat.S_ISDIR(info.st_mode))
            or (not expected_dir and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1))
            or (info.st_mode & 0o022 and not (current != path and info.st_mode & stat.S_ISVTX))
        ):
            raise LaunchRejected("launch artifact is replaceable")


def command_for(spec):
    """Construct the fixed command on either OS; this is not launch admission."""
    for key in ("bundle", "home"):
        _path(spec[key])
    profile = spec["profile"]
    if type(profile) is not str or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", profile):
        raise LaunchRejected("invalid policy profile")
    return [
        "/usr/bin/env",
        "-i",
        "PATH=/usr/bin:/bin",
        "LANG=C.UTF-8",
        "HOME=" + spec["home"],
        "CODEX_HOME=" + spec["home"],
        spec["bundle"] + "/bin/codex",
        "app-server",
        "--strict-config",
        "-c",
        'default_permissions="' + profile + '"',
        "-c",
        'approval_policy="never"',
    ]


def validate_spec(spec):
    """Validate portable syntax before host effects; no filesystem qualification."""
    if type(spec) is not dict or set(spec) != {
        "bundle",
        "policy_source",
        "policy_sha256",
        "home",
        "workspace",
        "profile",
        "home_size_mib",
    }:
        raise LaunchRejected("invalid launch specification")
    bundle, policy, home, workspace = (
        _path(spec[key]) for key in ("bundle", "policy_source", "home", "workspace")
    )
    if (
        type(spec["profile"]) is not str
        or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", spec["profile"])
        or type(spec["policy_sha256"]) is not str
        or not re.fullmatch(r"[0-9a-f]{64}", spec["policy_sha256"])
        or type(spec["home_size_mib"]) is not int
        or not 32 <= spec["home_size_mib"] <= 1024
    ):
        raise LaunchRejected("invalid policy identity or memory bound")
    if any(
        a == b or a in b.parents or b in a.parents
        for a, b in ((home, bundle), (home, policy), (home, workspace))
    ):
        raise LaunchRejected("private home overlaps another artifact")
    return bundle, policy, home, workspace


def prepare(spec, verify_bundle):
    if sys.platform != "linux" or os.geteuid() != 0:
        raise LaunchRejected("Linux root controller required")
    bundle, policy, home, workspace = validate_spec(spec)
    _protected(policy, directory=False)
    _protected(home, directory=True)
    if any(home.iterdir()):
        raise LaunchRejected("home placeholder must be empty")
    data = policy.read_bytes()
    if hashlib.sha256(data).hexdigest() != spec["policy_sha256"]:
        raise LaunchRejected("policy digest mismatch")
    parsed = tomllib.loads(data.decode("utf-8"))
    permissions = parsed.get("permissions")
    if type(permissions) is not dict:
        raise LaunchRejected("permissions table required")
    selected = permissions.get(spec["profile"])
    if type(selected) is not dict or type(selected.get("filesystem")) is not dict:
        raise LaunchRejected("filesystem profile required")
    network = selected.get("network")
    if type(network) is not dict or set(network) != {"enabled"} or network["enabled"] is not False:
        raise LaunchRejected("offline network policy required")
    if selected.get("filesystem", {}).get(str(workspace)) != "write":
        raise LaunchRejected("policy workspace mismatch")
    verification = verify_bundle(
        bundle, target="x86_64-unknown-linux-musl", require_root_owned=True
    )
    if (
        verification.root != bundle
        or verification.target != "x86_64-unknown-linux-musl"
        or verification.root_ownership_checked is not True
        or verification.permissions_checked is not True
    ):
        raise LaunchRejected("protected bundle unverified")
    return {
        "properties": [
            "PrivateNetwork=yes",
            "TemporaryFileSystem="
            + str(home)
            + ":rw,mode=0700,uid=1000,gid=1000,size="
            + str(spec["home_size_mib"])
            + "M",
            "BindReadOnlyPaths=" + str(policy) + ":" + str(home / "config.toml"),
        ],
        "command": command_for(
            {"bundle": str(bundle), "home": str(home), "profile": spec["profile"]}
        ),
        "working_directory": str(workspace),
    }


def protect_arguments(arguments, spec, verify_bundle):
    """Bind the helper's command to verified artifacts while its unit lock is held."""
    plan = prepare(spec, verify_bundle)
    if type(arguments) is not list or not arguments or arguments[0] != "/usr/bin/systemd-run":
        raise LaunchRejected("systemd launcher required")
    try:
        boundary = arguments.index("--")
    except ValueError:
        raise LaunchRejected("command boundary missing") from None
    if arguments[boundary + 1 :] != plan["command"]:
        raise LaunchRejected("requested command differs from verified Codex command")
    # Only helper-owned options precede the boundary. Caller-supplied options
    # cannot override the protected properties appended here.
    return [
        *arguments[:boundary],
        *("--property=" + value for value in plan["properties"]),
        "--working-directory=" + plan["working_directory"],
        "--",
        *plan["command"],
    ]
