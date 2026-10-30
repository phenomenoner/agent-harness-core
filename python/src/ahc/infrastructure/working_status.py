"""Read-only status projection for M2.2 working continuity."""

from __future__ import annotations

from pathlib import Path

from ahc.domain.working_values import WORKING_CONTINUITY_STATUS_SCHEMA_VERSION

from .working_db import (
    WORKING_DB_FILENAME,
    WORKING_LOCK_FILENAME,
    decode_snapshot,
    open_connection,
    require_real_directory,
    require_regular_file,
    require_schema,
)


def inspect_working_continuity(root: str | Path) -> dict[str, object]:
    """Observe one explicit repository without initialization or repair."""

    base = {"schema_version": WORKING_CONTINUITY_STATUS_SCHEMA_VERSION}
    try:
        root_path = require_real_directory(Path(root), allow_absent=True)
    except Exception:
        return {**base, "status": "degraded", "current_count": 0}
    if not root_path.exists():
        return {**base, "status": "not-initialized", "current_count": 0}
    db_path = root_path / WORKING_DB_FILENAME
    lock_path = root_path / WORKING_LOCK_FILENAME
    try:
        require_regular_file(db_path, allow_absent=True)
        require_regular_file(lock_path, allow_absent=True)
    except Exception:
        return {**base, "status": "degraded", "current_count": 0}
    if not db_path.exists():
        return {**base, "status": "not-initialized", "current_count": 0}
    try:
        connection = open_connection(db_path, readonly=True)
        try:
            require_schema(connection)
            current = int(
                connection.execute("SELECT COUNT(*) FROM current_working_sets").fetchone()[0]
            )
            snapshots = int(connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0])
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise RuntimeError("foreign-key integrity failed")
            rows = connection.execute(
                """SELECT c.generation, c.revision, c.agent_id, c.route_id,
                          c.snapshot_digest, s.snapshot_json
                   FROM current_working_sets c
                   JOIN snapshots s
                     ON s.virtual_session_id=c.virtual_session_id
                    AND s.generation=c.generation AND s.revision=c.revision
                    AND s.agent_id=c.agent_id AND s.route_id=c.route_id
                    AND s.snapshot_digest=c.snapshot_digest"""
            ).fetchall()
            if len(rows) != current:
                raise RuntimeError("current projection is incomplete")
            for row in rows:
                snapshot = decode_snapshot(row[5])
                if (
                    snapshot.session.ref.generation,
                    snapshot.revision,
                    snapshot.session.agent_id,
                    snapshot.session.route_id,
                    snapshot.snapshot_digest,
                ) != tuple(row[:5]):
                    raise RuntimeError("current projection disagrees with snapshot")
        finally:
            connection.close()
    except Exception:
        return {**base, "status": "degraded", "current_count": 0}
    return {
        **base,
        "status": "ready",
        "current_count": current,
        "snapshot_count": snapshots,
    }


__all__ = ["inspect_working_continuity"]
