"""Read-only observations of a process's effective cgroup-v2 resource limits.

This reports configured cgroup files for the process's current cgroup. It does
not establish that a kernel controller is enabled or enforcing those values.
"""

from __future__ import annotations

import errno
import os
import sys
from fractions import Fraction
from pathlib import Path

_CONTROLLERS = (
    ("memory", "memory.max"),
    ("pids", "pids.max"),
    ("cpu", "cpu.max"),
)
_PROC_FILE_LIMIT = 16_384
_LIMIT_FILE_LIMIT = 256
_CGROUP_PATH_LIMIT = 4_096
_MAX_DECIMAL_DIGITS = 64


def _read_fd(descriptor: int, maximum: int) -> bytes:
    value = bytearray()
    while len(value) <= maximum:
        block = os.read(descriptor, min(1024, maximum + 1 - len(value)))
        if not block:
            return bytes(value)
        value.extend(block)
    raise ValueError("observation file exceeds bound")


def _read_path(path: Path, maximum: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        return _read_fd(descriptor, maximum)
    finally:
        os.close(descriptor)


def _read_process_starttime(pid: int, proc_root: Path) -> tuple[int | None, str | None]:
    try:
        raw = _read_path(proc_root / str(pid) / "stat", _PROC_FILE_LIMIT)
    except OSError:
        return None, "unavailable"
    except ValueError:
        return None, "invalid"

    # comm is parenthesized but may itself contain spaces and closing parens.
    first_space = raw.find(b" ")
    open_paren = raw.find(b"(", first_space + 1) if first_space >= 0 else -1
    close_paren = raw.rfind(b")")
    if (
        first_space <= 0
        or raw[:first_space] != str(pid).encode("ascii")
        or open_paren != first_space + 1
        or close_paren <= open_paren
    ):
        return None, "invalid"
    fields = raw[close_paren + 1 :].split()
    # The first token after comm is field 3; starttime is field 22.
    if len(fields) < 20 or not fields[19].isdigit():
        return None, "invalid"
    try:
        return int(fields[19]), None
    except ValueError:
        return None, "invalid"


def _parse_cgroup_path(raw: bytes) -> tuple[str, tuple[str, ...]]:
    try:
        text = raw.decode("utf-8")
    except UnicodeError as error:
        raise ValueError("invalid-cgroup-file") from error
    if len(raw) > _PROC_FILE_LIMIT:
        raise ValueError("invalid-cgroup-file")

    v2_paths: list[str] = []
    for line in text.splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3:
            raise ValueError("invalid-cgroup-file")
        hierarchy, controllers, path = fields
        if not hierarchy.isascii() or not hierarchy.isdigit() or not path.startswith("/"):
            raise ValueError("invalid-cgroup-file")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in path):
            raise ValueError("invalid-cgroup-file")
        if hierarchy == "0":
            if controllers:
                raise ValueError("invalid-v2-entry")
            v2_paths.append(path)
    if len(v2_paths) != 1:
        raise ValueError("missing-or-duplicate-v2-entry")

    path = v2_paths[0]
    if (
        len(path) > _CGROUP_PATH_LIMIT
        or path.startswith("//")
        or "\\" in path
    ):
        raise ValueError("invalid-cgroup-path")
    parts = () if path == "/" else tuple(path[1:].split("/"))
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError("invalid-cgroup-path")
    return path, parts


def _read_limit_file(directory_fd: int, name: str) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=directory_fd)
    try:
        return _read_fd(descriptor, _LIMIT_FILE_LIMIT)
    finally:
        os.close(descriptor)


def _unknown(reason: str) -> dict[str, str]:
    return {"state": "unknown", "reason": reason}


def _decimal(token: str) -> int:
    if (
        not token.isascii()
        or not token.isdigit()
        or len(token) > _MAX_DECIMAL_DIGITS
    ):
        raise ValueError("invalid-decimal")
    return int(token)


def _controller_observation(directory_fd: int, key: str, filename: str, *, root: bool = False) -> dict[str, str]:
    try:
        raw = _read_limit_file(directory_fd, filename)
        text = raw.decode("ascii").strip()
    except OSError as error:
        # These max interfaces exist on non-root cgroups only. Missing root
        # files contribute no cap in the visible cgroup hierarchy. Missing
        # non-root files and all other errors remain explicitly unknown.
        if root and error.errno == errno.ENOENT:
            return {"state": "not-applicable", "reason": "root-cgroup-no-max-interface"}
        reason = "unsafe-path" if error.errno == errno.ELOOP else "unavailable"
        return _unknown(reason)
    except (UnicodeError, ValueError):
        return _unknown("invalid")

    fields = text.split()
    if key in ("memory", "pids"):
        if len(fields) != 1:
            return _unknown("invalid")
        if fields[0] == "max":
            return {"state": "unlimited"}
        try:
            return {"state": "finite", "value": str(_decimal(fields[0]))}
        except ValueError:
            return _unknown("invalid")

    if len(fields) != 2:
        return _unknown("invalid")
    try:
        period = _decimal(fields[1])
        if period == 0:
            return _unknown("invalid")
        if fields[0] == "max":
            return {"state": "unlimited"}
        quota = _decimal(fields[0])
    except ValueError:
        return _unknown("invalid")
    return {
        "state": "finite",
        "quota": str(quota),
        "period": str(period),
        "ratio": str(Fraction(quota, period)),
    }


def _effective(
    levels: list[dict[str, object]],
    controller: str,
    *,
    hierarchy_complete: bool,
) -> dict[str, str]:
    if not hierarchy_complete:
        return _unknown("depth-limit")
    observations = [
        level["controllers"][controller]  # type: ignore[index]
        for level in levels
    ]
    if any(item["state"] == "unknown" for item in observations):  # type: ignore[index]
        return _unknown("ancestor-unavailable")

    finite = [
        (level["path"], item)
        for level, item in zip(levels, observations, strict=True)
        if item["state"] == "finite"  # type: ignore[index]
    ]
    if not finite:
        return {"state": "unlimited"}
    if controller in ("memory", "pids"):
        source, selected = min(finite, key=lambda pair: int(pair[1]["value"]))  # type: ignore[index]
        return {"state": "finite", "value": selected["value"], "source": source}  # type: ignore[index]

    source, selected = min(
        finite,
        key=lambda pair: Fraction(int(pair[1]["quota"]), int(pair[1]["period"])),  # type: ignore[index]
    )
    return {
        "state": "finite",
        "quota": selected["quota"],  # type: ignore[index]
        "period": selected["period"],  # type: ignore[index]
        "ratio": selected["ratio"],  # type: ignore[index]
        "source": source,
    }


def _empty(pid: int, *, reason: str, identity_verified: bool = False) -> dict[str, object]:
    return {
        "pid": pid,
        "identity_verified": identity_verified,
        "status": "unavailable",
        "reason": reason,
        "cgroup_path": None,
        "leaf": None,
        "ancestors": [],
        "hierarchy_complete": False,
        "effective_limits": {
            key: _unknown("unavailable") for key, _ in _CONTROLLERS
        },
    }


def _observe_cgroup(
    cgroup_path: str,
    parts: tuple[str, ...],
    cgroup_root: Path,
    max_depth: int,
) -> dict[str, object]:
    if sys.platform != "linux" or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        result = _empty(0, reason="linux-descriptor-observation-unavailable",
                        identity_verified=True)
        result["cgroup_path"] = cgroup_path
        return result

    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    descriptors: list[tuple[str, int]] = []
    try:
        root_fd = os.open(cgroup_root, flags)
        descriptors.append(("/", root_fd))
        current_fd = root_fd
        for index, part in enumerate(parts, start=1):
            child_fd = os.open(part, flags, dir_fd=current_fd)
            path = "/" + "/".join(parts[:index])
            descriptors.append((path, child_fd))
            current_fd = child_fd
            if len(descriptors) > max_depth:
                _, old_fd = descriptors.pop(0)
                os.close(old_fd)

        hierarchy_complete = len(parts) + 1 <= max_depth
        levels = []
        for path, directory_fd in reversed(descriptors):
            levels.append(
                {
                    "path": path,
                    "controllers": {
                        key: _controller_observation(directory_fd, key, filename, root=path == "/")
                        for key, filename in _CONTROLLERS
                    },
                }
            )
        leaf = levels[0]
        ancestors = levels[1:]
        result: dict[str, object] = {
            "pid": 0,
            "identity_verified": True,
            "status": (
                "observed"
                if hierarchy_complete
                and all(
                    observation["state"] != "unknown"
                    for level in levels
                    for observation in level["controllers"].values()
                )
                else "partial"
            ),
            "cgroup_path": cgroup_path,
            "leaf": leaf,
            "ancestors": ancestors,
            "hierarchy_complete": hierarchy_complete,
            "effective_limits": {
                key: _effective(levels, key, hierarchy_complete=hierarchy_complete)
                for key, _ in _CONTROLLERS
            },
        }
        if not hierarchy_complete:
            result["reason"] = "depth-limit"
        elif result["status"] == "partial":
            result["reason"] = "controller-observation-incomplete"
        return result
    except OSError as error:
        reason = "cgroup-path-unavailable-or-unsafe"
        if error.errno == errno.ELOOP:
            reason = "cgroup-symlink-rejected"
        result = _empty(0, reason=reason, identity_verified=True)
        result["cgroup_path"] = cgroup_path
        return result
    finally:
        for _, descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass


def observe_process_resources(
    pid: int,
    *,
    proc_root: Path = Path("/proc"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    max_depth: int = 32,
) -> dict[str, object]:
    """Observe limits for one exact Linux PID generation without changing it.

    Effective memory and PID limits are reported only when every cgroup from
    leaf through root was observed. CPU limits use exact rational strings.
    Unknown controller reads remain unknown; no result asserts enforcement.
    """
    if type(pid) is not int or pid <= 0:
        raise ValueError("pid must be a positive exact integer")
    if type(max_depth) is not int or not 1 <= max_depth <= 128:
        raise ValueError("max_depth must be an integer from 1 through 128")

    proc_root = Path(proc_root)
    cgroup_root = Path(cgroup_root)
    starttime, reason = _read_process_starttime(pid, proc_root)
    if starttime is None:
        return _empty(pid, reason="process-" + (reason or "unavailable"))

    try:
        raw_cgroup = _read_path(proc_root / str(pid) / "cgroup", _PROC_FILE_LIMIT)
        cgroup_path, parts = _parse_cgroup_path(raw_cgroup)
        result = _observe_cgroup(cgroup_path, parts, cgroup_root, max_depth)
    except OSError:
        result = _empty(pid, reason="process-cgroup-unavailable", identity_verified=True)
    except ValueError as error:
        result = _empty(pid, reason=str(error), identity_verified=True)

    after, _ = _read_process_starttime(pid, proc_root)
    if after is None or after != starttime:
        return _empty(pid, reason="process-identity-changed-or-unavailable")
    result["pid"] = pid
    result["identity_verified"] = True
    return result


__all__ = ["observe_process_resources"]
