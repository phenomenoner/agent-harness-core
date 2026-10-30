"""Pure legacy-note normalization, without migration or serving authority.

The archive reader supplies immutable originals. This module recognizes two
historical record formats and proposes observations for an explicitly selected
owner. An operator still has to admit scope, disposition and duplicate handling.
"""

import base64
import json
import re

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json

from .memory_note_archive import _nonfinite, _unique_object


def note_candidate(original, *, expected_owner):
    """Return a candidate or an unresolved reason; never a serving decision.

    Paths, quoted instructions and session labels cannot establish ownership.
    Explicit legacy scopes require a separate mapping, including a scope whose
    spelling happens to match the destination owner. No content-based deduplication
    is attempted: repeated text can represent distinct historical events.
    """
    if (
        type(expected_owner) is not str
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", expected_owner)
        or type(original) is not dict
    ):
        raise ContractError("note candidate owner or original invalid")
    result = {"status": "unresolved", "original_digest": digest_json(original),
              "owner": None, "observation": None}

    def unresolved(reason):
        return {**result, "reason": reason}

    if original.get("kind") != "jsonl-record":
        return unresolved("file-custody-only")
    if original.get("parse_status") != "parsed":
        return unresolved("invalid-json")
    # Bind interpretation to the preserved line, not a mutable parsed projection.
    try:
        raw = base64.b64decode(original["bytes_base64"], validate=True)
        parsed = json.loads(raw.decode("utf-8-sig"), parse_constant=_nonfinite,
                            object_pairs_hook=_unique_object)
        if canonical_json(parsed) != canonical_json(original.get("parsed")):
            return unresolved("parsed-original-mismatch")
    except (KeyError, TypeError, ValueError, UnicodeError, ContractError):
        return unresolved("invalid-json")
    if type(parsed) is not dict:
        return unresolved("unsupported-record-format")
    sections = [parsed] + [parsed[k] for k in ("detail", "payload")
                           if type(parsed.get(k)) is dict]
    owners = [section[k] for section in sections for k in ("agent_id", "agentId")
              if k in section]
    top_owners = [parsed[k] for k in ("agent_id", "agentId") if k in parsed]
    if not top_owners or any(type(owner) is not str or not owner for owner in owners):
        return unresolved("owner-unassigned")
    if any(owner != top_owners[0] for owner in owners):
        return unresolved("owner-conflict")
    result["owner"] = top_owners[0]
    if result["owner"] != expected_owner:
        return unresolved("other-owner")
    if any(section.get("scope") is not None for section in sections):
        return unresolved("explicit-scope-needs-mapping")
    if parsed.get("schema") == "openclaw-mem.service-store.v1":
        kind, summary = "legacy-service-store", parsed.get("text")
        if type(parsed.get("payload")) is not dict:
            return unresolved("unsupported-record-format")
    elif "schema" not in parsed and type(parsed.get("detail")) is dict:
        kind, summary = parsed.get("kind"), parsed.get("summary")
    else:
        return unresolved("unsupported-record-format")
    if any(type(value) is not str or not value.strip() for value in (kind, summary)):
        return unresolved("empty-or-invalid-content")
    # Keep synthetic flags, tool/turn references and every legacy detail intact.
    # These remain historical data, not current identity or permission fields.
    observation = {"kind": kind, "summary": summary, "detail": {
        "legacy_record": parsed,
        "note_origin": {key: original.get(key) for key in (
            "path", "container_key", "file_sha256", "line", "byte_offset")},
    }}
    if len(canonical_json(observation)) > 60 * 1024:
        return unresolved("observation-needs-bounded-splitting")
    return {**result, "status": "candidate", "reason": "recognized-unscoped-owner",
            "observation": observation}


__all__ = ["note_candidate"]
