"""Pure proposals for verified legacy snapshots under operator source mappings.

The caller verifies snapshot identity and supplies document admission. This
module grants no serving scope, imports nothing, and never executes old events.
"""

import json

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

_INVALID_JSON = object()


def _json_value(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    if type(raw) is not str:
        return _INVALID_JSON
    try:
        return json.loads(
            raw,
            object_pairs_hook=unique,
            parse_constant=lambda _v: (_ for _ in ()).throw(ValueError("nonfinite")),
        )
    except (ValueError, TypeError, RecursionError):
        return _INVALID_JSON


def _json_object(raw):
    value = _json_value(raw)
    return value if type(value) is dict else None


def classify_legacy_record(
    original, *, owner, scope, document_source_admitted=False, event_scopes=()
):
    """Return a candidate decision; absent source admission stays blocked.

    Event agent_id is authoritative source metadata; global is not inferred to
    mean main. Its mapping is separately admitted for this exact owner. Document
    filenames, prose and project names alone never imply document admission.
    Full originals remain in MemoryMigration; derived embeddings are not copied
    into normalized semantic text. A caller must split an oversized observation.
    """
    if (
        type(original) is not dict
        or type(original.get("row")) is not dict
        or type(owner) is not str
        or not owner
        or type(scope) is not str
        or not scope
        or type(document_source_admitted) is not bool
        or type(event_scopes) is not tuple
        or any(type(value) is not str for value in event_scopes)
    ):
        raise ContractError("legacy classification input invalid")
    table, row = original.get("table"), original["row"]

    def decision(disposition, reason, observation=None):
        return {
            "disposition": disposition,
            "reason": reason,
            "scope": scope if disposition == "serve" else None,
            "observation": observation,
        }

    def serve(kind, text, detail):
        if type(text) is not str or not text.strip():
            return decision("archive", "Nonsemantic source metadata; full original retained")
        observation = {"kind": kind, "summary": text, "detail": detail}
        if len(canonical_json(observation)) > 60000:
            return decision("blocked", "Mapped owner; normalization needs bounded splitting")
        return decision("serve", "Exact owner or admitted document-source mapping", observation)

    if table == "observations":
        detail = _json_object(row.get("detail_json"))
        if detail is None:
            return decision("blocked", "Invalid structured ownership metadata")
        names = [detail[key] for key in ("agent_id", "agentId") if detail.get(key) is not None]
        if names and (
            any(type(name) is not str or not name for name in names) or len(set(names)) != 1
        ):
            return decision("blocked", "Conflicting structured owner metadata")
        source_scope = detail.get("scope")
        if (names and names[0] != owner) or (source_scope is not None and source_scope != scope):
            return decision("archive", "Explicit other owner or scope; no cross-agent transfer")
        exact = (names == [owner] or bool(names) and set(names) == {owner}) or source_scope == scope
        if not exact and not document_source_admitted:
            return decision("blocked", "Document provenance needs an explicit source mapping")
        return serve(
            row.get("kind", "legacy-observation"),
            row.get("summary"),
            {
                "legacy_detail": detail,
                "legacy_observation_id": row.get("id"),
                "legacy_timestamp": row.get("ts"),
            },
        )
    if table == "episodic_events":
        agent = row.get("agent_id")
        if agent in (None, "", "unknown"):
            return decision(
                "blocked", "Unknown event owner; source mapping does not assign conversations"
            )
        if agent != owner:
            return decision("archive", "Explicit other event owner; no cross-agent transfer")
        if row.get("scope") not in event_scopes:
            return decision("blocked", "Explicit event scope needs its own mapping")
        # Event ownership belongs to the row, not the shape of its historical
        # payload. Arrays, scalar values and null are valid original JSON too.
        payload = _json_value(row.get("payload_json"))
        return serve(
            "legacy-episode",
            row.get("summary"),
            {
                "legacy_event": {key: value for key, value in row.items() if key != "search_text"},
                "legacy_type": row.get("type"),
                "payload_format": (
                    "absent"
                    if row.get("payload_json") is None
                    else "invalid-json-retained-verbatim"
                    if payload is _INVALID_JSON
                    else "valid-json-retained-verbatim"
                ),
            },
        )
    if table in ("docs_chunks", "chunks"):
        if not document_source_admitted:
            return decision("blocked", "Document provenance needs an explicit source mapping")
        metadata = {key: value for key, value in row.items() if key not in ("text", "embedding")}
        return serve("legacy-document", row.get("text"), {"legacy_document": metadata})
    if table in (
        "files",
        "meta",
        "graph_nodes",
        "graph_edges",
        "graph_facts",
        "native_identity",
        "native_operations",
        "ahc_legacy_main_import_v1",
    ):
        return decision(
            "archive", "Retained metadata or derived navigation; not an independent memory"
        )
    return decision("blocked", "Unsupported source table; original retained")


__all__ = ["classify_legacy_record"]
