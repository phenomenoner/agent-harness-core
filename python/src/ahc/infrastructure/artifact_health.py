"""Metadata-only artifact counts for one exact producer session."""

import sqlite3


def inspect_artifact_counts(connection, session_json, tables):
    if "runtime_artifacts" not in tables:
        return {"status": "schema-absent"}
    try:
        # An unparseable manifest cannot safely be assigned to any session.
        invalid = connection.execute("SELECT count(*) FROM runtime_artifacts "
            "WHERE NOT json_valid(manifest_json)").fetchone()[0]
        if invalid:
            return {"status": "unreadable"}
        invalid = connection.execute("SELECT count(*) FROM runtime_artifacts WHERE "
            "json_type(manifest_json,'$.producer.session') IS NOT 'object'").fetchone()[0]
        if invalid:
            return {"status": "unreadable"}
        count, size = connection.execute("SELECT count(*),COALESCE(sum(length(content)),0) "
            "FROM runtime_artifacts WHERE json_extract(manifest_json,'$.producer.session')=?",
            (session_json,)).fetchone()
        return {"status": "observed", "state_counts": {"recorded": count},
                "stored_bytes": size, "content_verified": False,
                "ownership_scope": "exact-producer-session"}
    except sqlite3.DatabaseError:
        return {"status": "unreadable"}
