"""Bounded metadata from the same authorized catalog used by model skill tools."""

import json
import sqlite3

from ahc.domain.sessions import ContractError

DISCOVERY_TOOLS = frozenset({"workspace.skills_list", "workspace.skills_read"})
MAX_DISCOVERY_BYTES = 8192
MAX_DISCOVERY_SKILLS = 16


def skill_discovery_snapshot(tools, request):
    snapshot = {
        "schema_version": "ahc.skill-discovery.v1",
        "source": "authorized_generation_pin",
        "selection": "model-discretion",
        "status": "disabled",
        "skills": [],
    }
    if tools is None or not DISCOVERY_TOOLS.issubset(request.capabilities):
        return snapshot
    if not DISCOVERY_TOOLS.issubset(tools.capabilities()):
        return snapshot
    identity = tools.workspace.identity
    if identity.session != request.session:
        raise ContractError("skill discovery context owner mismatch")
    try:
        observed = tools.invoke("workspace.skills_list", {
            "expected": identity.to_dict(), "limit": MAX_DISCOVERY_SKILLS,
        })
    except (ContractError, OSError, UnicodeError, sqlite3.Error):
        observed = None
    if observed is None or observed.get("disposition") != "observed":
        snapshot["status"] = "unavailable"
        return snapshot
    listing = observed["result"]
    snapshot.update(status="available", total=listing["total"], next_offset=0)
    for item in listing["skills"]:
        row = {key: item[key] for key in ("skill_name", "revision_digest", "description")}
        snapshot["skills"].append(row)
        snapshot["next_offset"] = len(snapshot["skills"])
        if len(json.dumps(snapshot, ensure_ascii=False).encode("utf-8")) > MAX_DISCOVERY_BYTES:
            snapshot["skills"].pop()
            break
    count = len(snapshot["skills"])
    snapshot["next_offset"] = count if count < listing["total"] else None
    if snapshot["next_offset"] is not None:
        snapshot["status"] = "partial"
    return snapshot
