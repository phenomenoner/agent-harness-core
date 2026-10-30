"""Semantic projection and scope-bound vector reuse, separate from originals."""

import json

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json

INDEX_POLICY = "ahc.semantic-memory-index.v1"
# These are transport/import coordinates, not remembered facts. Exact originals
# and reference resolution retain them; arbitrary user detail fields remain text.
OPERATIONAL_DETAIL = frozenset(
    {
        "scope",
        "agent_id",
        "migration_source",
        "migration_key",
        "original_digest",
        "legacy_timestamp",
        "legacy_observation_id",
        "native_observation_id",
        "tool_call_id",
        "session_key",
        "memory_backend",
        "memory_backend_mode",
        "memory_backend_ready",
        "memory_tool",
        "memory_operation",
        "is_synthetic",
    }
)


def semantic_text(observation):
    detail = {
        key: value for key, value in observation["detail"].items() if key not in OPERATIONAL_DETAIL
    }
    return (
        observation["kind"] + "\n" + observation["summary"] + "\n" + canonical_json(detail).decode()
    )


def prepare(db):
    row = db.execute("SELECT value FROM meta WHERE key='index_policy'").fetchone()
    if row is None:
        if db.execute("SELECT 1 FROM chunks LIMIT 1").fetchone():
            raise ContractError("previous vector policy requires explicit projection rebuild")
        db.execute("INSERT INTO meta VALUES('index_policy',?)", (INDEX_POLICY,))
    elif row[0] != INDEX_POLICY:
        raise ContractError("memory vector policy requires explicit projection rebuild")
    db.execute(
        "CREATE TABLE IF NOT EXISTS embedding_cache ("
        "scope TEXT NOT NULL, content_digest TEXT NOT NULL, content TEXT NOT NULL, "
        "vector TEXT NOT NULL, integrity TEXT NOT NULL, PRIMARY KEY(scope,content_digest))"
    )
    # Recovery visits every record. Without these additive indexes, each chunk
    # lookup scans the corpus and each pending batch rescans completed rows.
    # Prepare runs under the journal writer, including for pre-index backups.
    db.execute("CREATE INDEX IF NOT EXISTS chunks_record_ref_id ON chunks(record_ref,id)")
    db.execute(
        "CREATE INDEX IF NOT EXISTS records_scope_indexed_id ON records(scope,indexed,id)"
    )


def cached_vector(db, *, scope, text, embedding_identity):
    row = db.execute(
        "SELECT content,vector,integrity FROM embedding_cache WHERE scope=? AND content_digest=?",
        (scope, digest_json(text)),
    ).fetchone()
    if row is None:
        return None
    vector = json.loads(row[1])
    if row[0] != text or row[2] != digest_json([embedding_identity, scope, text, vector]):
        raise ContractError("cached memory vector integrity changed")
    return vector


def save_vector(db, *, scope, text, vector, embedding_identity):
    db.execute(
        "INSERT INTO embedding_cache VALUES(?,?,?,?,?)",
        (
            scope,
            digest_json(text),
            text,
            canonical_json(vector).decode(),
            digest_json([embedding_identity, scope, text, vector]),
        ),
    )
