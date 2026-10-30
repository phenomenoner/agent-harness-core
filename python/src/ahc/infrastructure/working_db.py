"""SQLite schema and connection helpers for M2.2 working continuity."""

from __future__ import annotations

import json
import sqlite3
import stat
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.domain.working_sets import WorkingSetSnapshot

WORKING_DB_SCHEMA_VERSION = 1
WORKING_DB_FILENAME = "working.db"
WORKING_LOCK_FILENAME = "writer.lock"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots (
  virtual_session_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  route_id TEXT NOT NULL,
  snapshot_digest TEXT NOT NULL UNIQUE,
  snapshot_json TEXT NOT NULL,
  PRIMARY KEY (virtual_session_id, generation, revision),
  UNIQUE (virtual_session_id, generation, revision, agent_id, route_id, snapshot_digest)
);
CREATE TABLE IF NOT EXISTS current_working_sets (
  virtual_session_id TEXT PRIMARY KEY,
  generation INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  route_id TEXT NOT NULL,
  snapshot_digest TEXT NOT NULL UNIQUE,
  FOREIGN KEY (virtual_session_id, generation, revision, agent_id, route_id, snapshot_digest)
    REFERENCES snapshots(
      virtual_session_id, generation, revision, agent_id, route_id, snapshot_digest
    )
);
"""


class WorkingDatabaseError(RuntimeError):
    """Working continuity storage is absent, corrupt, busy, or unsupported."""


def initialize_connection(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(_SCHEMA)
    connection.execute(f"PRAGMA user_version = {WORKING_DB_SCHEMA_VERSION}")
    connection.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
        (str(WORKING_DB_SCHEMA_VERSION),),
    )


def open_connection(
    path: Path, *, readonly: bool, allow_create: bool = False
) -> sqlite3.Connection:
    if (path.parent / "sidecar-pending.json").exists():
        raise WorkingDatabaseError("sidecar transition requires reconciliation")
    try:
        require_regular_file(path, allow_absent=allow_create)
        if readonly:
            uri = f"{path.absolute().as_uri()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=0.0)
            connection.execute("PRAGMA query_only = ON")
        else:
            connection = sqlite3.connect(path, isolation_level=None, timeout=0.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
    except sqlite3.Error as exc:
        raise WorkingDatabaseError("working continuity database cannot be opened") from exc


def require_regular_file(path: Path, *, allow_absent: bool) -> None:
    require_real_directory(path.parent, allow_absent=False)
    try:
        info = path.lstat()
    except FileNotFoundError:
        if allow_absent:
            return
        raise WorkingDatabaseError("working continuity artifact is absent") from None
    except OSError as exc:
        raise WorkingDatabaseError("working continuity artifact is unavailable") from exc
    if _is_reparse(info) or not stat.S_ISREG(info.st_mode):
        raise WorkingDatabaseError("working continuity artifact must be a regular file")


def require_real_directory(path: Path, *, allow_absent: bool) -> Path:
    absolute = path.absolute()
    for component in (*reversed(absolute.parents), absolute):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise WorkingDatabaseError("working continuity root is unavailable") from exc
        if _is_reparse(info) or not stat.S_ISDIR(info.st_mode):
            raise WorkingDatabaseError("working continuity root must contain no reparse path")
    if not allow_absent and not absolute.is_dir():
        raise WorkingDatabaseError("working continuity root is absent")
    return absolute


def _is_reparse(info: object) -> bool:
    mode = getattr(info, "st_mode", 0)
    attributes = getattr(info, "st_file_attributes", 0)
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return stat.S_ISLNK(mode) or bool(attributes & reparse)


def require_schema(connection: sqlite3.Connection) -> None:
    try:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        meta = connection.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except (sqlite3.Error, TypeError, ValueError) as exc:
        raise WorkingDatabaseError("working continuity schema is unreadable") from exc
    if version != WORKING_DB_SCHEMA_VERSION or meta is None or meta[0] != str(version):
        raise WorkingDatabaseError("working continuity schema version is unsupported")


def sqlite_is_busy(error: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, sqlite3.Error):
            code = getattr(current, "sqlite_errorcode", 0)
            if bool(code) and code & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                return True
        current = current.__cause__
    return False


def encode_snapshot(snapshot: WorkingSetSnapshot) -> str:
    return canonical_json(snapshot.to_dict()).decode("utf-8")


def insert_snapshot(connection: sqlite3.Connection, snapshot: WorkingSetSnapshot) -> None:
    connection.execute(
        "INSERT INTO snapshots VALUES(?,?,?,?,?,?,?)",
        (*snapshot_pointer(snapshot), encode_snapshot(snapshot)),
    )


def snapshot_pointer(snapshot: WorkingSetSnapshot) -> tuple[str, int, int, str, str, str]:
    return (
        snapshot.session.ref.virtual_session_id,
        snapshot.session.ref.generation,
        snapshot.revision,
        snapshot.session.agent_id,
        snapshot.session.route_id,
        snapshot.snapshot_digest or "",
    )


def decode_snapshot(value: object) -> WorkingSetSnapshot:
    if type(value) is not str:
        raise WorkingDatabaseError("working-set snapshot is not canonical JSON text")
    try:
        parsed = json.loads(value, object_pairs_hook=_without_duplicates)
        return WorkingSetSnapshot.from_dict(parsed)
    except (ValueError, ContractError, json.JSONDecodeError) as exc:
        raise WorkingDatabaseError("working-set snapshot is invalid") from exc


def _without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


__all__ = [
    "WORKING_DB_FILENAME",
    "WORKING_DB_SCHEMA_VERSION",
    "WORKING_LOCK_FILENAME",
    "WorkingDatabaseError",
    "decode_snapshot",
    "encode_snapshot",
    "insert_snapshot",
    "require_real_directory",
    "require_regular_file",
    "sqlite_is_busy",
    "snapshot_pointer",
]
