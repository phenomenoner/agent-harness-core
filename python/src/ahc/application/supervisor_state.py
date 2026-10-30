"""State projection owned by the foreground supervisor."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

from ahc.domain import ProcessIdentity

if TYPE_CHECKING:
    from .supervisor import _Child


def build_state_snapshot(
    *,
    status: str,
    identity: ProcessIdentity,
    children: Mapping[str, _Child],
    started_at: float,
    failure: str | None,
) -> dict[str, object]:
    """Project supervisor-owned process state without mutating it."""
    child_snapshots: dict[str, object] = {}
    for name, child in children.items():
        process = child.process
        running = process is not None and process.poll() is None
        if running:
            child_status = "running"
        elif child.managed is None and child.return_code is None:
            child_status = "failed"
        else:
            child_status = "failed" if child.return_code else "stopped"
        child_snapshots[name] = {
            "name": name,
            "status": child_status,
            "identity": None if child.identity is None else child.identity.to_dict(),
            "restarts": child.restarts,
            "return_code": child.return_code,
        }
    snapshot: dict[str, object] = {
        "schema_version": 1,
        "status": status,
        "supervisor": identity.to_dict(),
        "children": child_snapshots,
        "started_at": started_at,
        "updated_at": time.time(),
    }
    if failure:
        snapshot["error"] = failure
    return snapshot
