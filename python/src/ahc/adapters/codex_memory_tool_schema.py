"""Visible argument shapes for the native memory tool; runtime validation stays authoritative."""


def exact_value_schema(value):
    if isinstance(value, dict):
        # The pinned vendor lowers const into enum; object enum values are not
        # provider-compatible. Closed objects with required exact leaves retain
        # the same identity constraint without the redundant composite const.
        return {"type": "object", "additionalProperties": False,
                "required": list(value),
                "properties": {key: exact_value_schema(item) for key, item in value.items()}}
    kind = "null" if value is None else "integer" if type(value) is int else "string"
    return {"type": kind, "const": value}


def native_memory_payload_schema(*, revisions=False):
    schema = {
        "type": "object", "additionalProperties": False,
        "description": (
            "Send only the fields required by the selected action. status: {}; "
            "get: {record_ref}; resolve: {row_id}; propose: {operation_id, observation, "
            "origin, expires_at}; execute/reconcile: {proposal_digest}; "
            "store: {operation_id, observation}. Observation detail must be a JSON object, "
            "for example {source: 'user instruction'}, never a string. "
            "Store requires explicitly configured trusted policy; propose/execute requires "
            "separate owner review. This tool cannot approve a proposal."
        ),
        "properties": {
            "operation_id": {"type": "string", "minLength": 1},
            "observation": {
                "type": "object", "additionalProperties": False,
                "required": ["kind", "summary", "detail"],
                "properties": {
                    "kind": {"type": "string", "minLength": 1},
                    "summary": {"type": "string", "minLength": 1},
                    "detail": {"type": "object"},
                },
            },
            "record_ref": {"type": "string", "minLength": 1},
            "row_id": {"type": "integer", "minimum": 1},
            "origin": {"type": "string", "minLength": 1},
            "expires_at": {"type": "number", "description": "Unix seconds"},
            "proposal_digest": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        },
    }
    if revisions:
        schema["description"] += (
            " revise: {operation_id, expected_ref, observation}; "
            "tombstone: {operation_id, expected_ref}; "
            "reconcile_revision: {revision_digest}. Revisions require a separate trusted "
            "memory.revise grant and the exact current record reference. Tombstones withdraw "
            "recall while retaining historical originals; they do not erase backup bytes. "
            "Use reconciliation after an unknown outcome; do not invent a new operation ID "
            "to resend it."
        )
        schema["properties"].update({
            "expected_ref": {"type": "string", "minLength": 1, "maxLength": 256},
            "revision_digest": {"type": "string", "pattern": "^[a-f0-9]{64}$"},
        })
    return schema
