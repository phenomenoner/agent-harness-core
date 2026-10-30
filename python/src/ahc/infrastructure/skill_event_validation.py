"""Strict semantic binding for surfaced, disclosed, and used events."""

from __future__ import annotations

import json
import sqlite3

from ahc.domain.sessions import ContractError
from ahc.domain.skill_records import SkillEpisode, SkillManifest

_EVENT_KINDS = frozenset({"surfaced", "disclosed", "used"})


def validate_event_binding(
    connection: sqlite3.Connection,
    *,
    event_key: str,
    episode_id: str,
    event_kind: str,
    revision_digest: str | None,
    manifest_digest: str | None,
    payload: dict[str, object],
) -> None:
    if event_kind not in _EVENT_KINDS or revision_digest is None or manifest_digest is None:
        raise ContractError("event kind and revision/manifest bindings are required")
    row = connection.execute(
        "SELECT episode_json FROM episodes WHERE episode_id = ?", (episode_id,)
    ).fetchone()
    if row is None:
        raise ContractError("event references an unknown episode")
    episode = SkillEpisode.from_dict(json.loads(row[0]))
    if manifest_digest != episode.manifest_digest:
        raise ContractError("event manifest does not match the stored episode")
    sets = {
        "surfaced": episode.surfaced_revisions,
        "disclosed": episode.disclosed_revisions,
        "used": episode.used_revisions,
    }
    if revision_digest not in sets[event_kind]:
        raise ContractError("event revision is not present in the stored episode evidence")
    manifest_row = connection.execute(
        "SELECT manifest_json FROM manifests WHERE manifest_digest = ?", (manifest_digest,)
    ).fetchone()
    if manifest_row is None:
        raise ContractError("event references an unknown manifest")
    manifest = SkillManifest.from_dict(json.loads(manifest_row[0]))
    entries = [entry for entry in manifest.entries if entry.revision_digest == revision_digest]
    if event_kind == "used":
        expected_payload = {"revision_digest": revision_digest}
        expected_key = f"{episode_id}:used:{revision_digest}"
    else:
        role = payload.get("role")
        if type(role) is not str or not any(entry.role == role for entry in entries):
            raise ContractError("event role is not bound by the stored manifest")
        expected_payload = {"role": role, "revision_digest": revision_digest}
        expected_key = f"{episode_id}:{event_kind}:{revision_digest}:{role}"
    if payload != expected_payload or event_key != expected_key:
        raise ContractError("event key or payload does not match canonical evidence")


__all__ = ["validate_event_binding"]
