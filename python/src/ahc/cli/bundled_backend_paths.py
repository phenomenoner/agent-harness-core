"""Deterministic workspace-owned paths; no installation or authentication discovery."""

import posixpath

from ahc.adapters.codex_linux_bundle import LINUX_CODEX_PIN
from ahc.domain.sessions import ContractError


def bundled_backend_paths(workspace):
    if (type(workspace) is not str or workspace == "/" or not workspace.startswith("/")
            or workspace.startswith("//") or "\\" in workspace
            or posixpath.normpath(workspace) != workspace
            or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in workspace)):
        raise ContractError("bundled backend requires a normalized WSL workspace")
    base = workspace + "/.ahc"
    release = LINUX_CODEX_PIN["version"] + "-" + LINUX_CODEX_PIN["target"]
    return {"bundle": base + "/tools/codex/releases/" + release,
            "codex_home": base + "/codex-home"}
