"""SQLite v2 schema; legacy stores remain readable and are never auto-upgraded."""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 2

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS revisions (
  revision_digest TEXT PRIMARY KEY,
  skill_name TEXT NOT NULL,
  version TEXT NOT NULL,
  cognitive_layer TEXT NOT NULL,
  body_digest TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  path TEXT NOT NULL UNIQUE,
  predecessor_digest TEXT REFERENCES revisions(revision_digest),
  created_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS active_revisions (
  skill_name TEXT PRIMARY KEY,
  revision_digest TEXT NOT NULL REFERENCES revisions(revision_digest),
  predecessor_digest TEXT REFERENCES revisions(revision_digest),
  updated_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS relationships (
  revision_digest TEXT NOT NULL REFERENCES revisions(revision_digest),
  kind TEXT NOT NULL,
  target_digest TEXT NOT NULL REFERENCES revisions(revision_digest),
  PRIMARY KEY (revision_digest, kind, target_digest)
);
CREATE TABLE IF NOT EXISTS manifests (
  manifest_digest TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  turn_id TEXT NOT NULL,
  goal_digest TEXT,
  manifest_json TEXT NOT NULL,
  request_digest TEXT,
  created_seq INTEGER NOT NULL,
  UNIQUE (session_id, generation, turn_id)
);
CREATE TABLE IF NOT EXISTS revision_availability (
  session_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  pin_json TEXT NOT NULL,
  pin_digest TEXT NOT NULL,
  PRIMARY KEY (session_id, generation)
);
CREATE TABLE IF NOT EXISTS episodes (
  episode_id TEXT PRIMARY KEY,
  manifest_digest TEXT NOT NULL REFERENCES manifests(manifest_digest),
  episode_json TEXT NOT NULL,
  outcome TEXT NOT NULL,
  attribution_digest TEXT,
  created_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_key TEXT NOT NULL UNIQUE,
  episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
  event_kind TEXT NOT NULL,
  revision_digest TEXT,
  manifest_digest TEXT,
  payload_json TEXT NOT NULL,
  created_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS beliefs (
  revision_digest TEXT NOT NULL REFERENCES revisions(revision_digest),
  agent_id TEXT NOT NULL,
  role TEXT NOT NULL,
  successes INTEGER NOT NULL DEFAULT 0,
  failures INTEGER NOT NULL DEFAULT 0,
  prior_alpha REAL NOT NULL DEFAULT 1.0,
  prior_beta REAL NOT NULL DEFAULT 1.0,
  expected_cost REAL NOT NULL DEFAULT 0.0,
  PRIMARY KEY (revision_digest, agent_id, role)
);
CREATE TABLE IF NOT EXISTS contributions (
  episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
  outcome TEXT NOT NULL,
  revision_digest TEXT NOT NULL REFERENCES revisions(revision_digest),
  role TEXT NOT NULL,
  PRIMARY KEY (episode_id, outcome, revision_digest, role)
);
CREATE TABLE IF NOT EXISTS proposals (
  proposal_digest TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
  candidate_digest TEXT NOT NULL REFERENCES revisions(revision_digest),
  attribution_digest TEXT NOT NULL,
  proposal_json TEXT NOT NULL,
  created_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS evaluations (
  evaluation_digest TEXT PRIMARY KEY,
  candidate_digest TEXT NOT NULL REFERENCES revisions(revision_digest),
  input_digest TEXT NOT NULL,
  verdict TEXT NOT NULL,
  report_json TEXT NOT NULL,
  created_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS receipts (
  operation_key TEXT PRIMARY KEY,
  operation_kind TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  candidate_digest TEXT,
  expected_digest TEXT,
  predecessor_digest TEXT,
  evaluation_digest TEXT,
  receipt_json TEXT NOT NULL,
  created_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS judgment_records (
  record_digest TEXT PRIMARY KEY,
  record_kind TEXT NOT NULL,
  record_json TEXT NOT NULL,
  created_seq INTEGER NOT NULL
);
"""


class DatabaseError(RuntimeError):
    """The durable authority is absent or cannot be opened."""


def initialize_connection(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA user_version").fetchone()[0] not in (0, SCHEMA_VERSION):
        raise DatabaseError("legacy skill store requires explicit conversion; no automatic upgrade")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(_SCHEMA)
    connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    connection.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )


def open_connection(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        if not path.is_file():
            raise DatabaseError("skill state is not initialized")
        uri = f"file:{path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        connection.execute("PRAGMA query_only = ON")
    else:
        connection = sqlite3.connect(path, isolation_level=None)
    connection.row_factory = sqlite3.Row
    if not readonly and connection.execute("PRAGMA user_version").fetchone()[0] not in (
        0, SCHEMA_VERSION,
    ):
        connection.close()
        raise DatabaseError("legacy skill store requires explicit conversion; no automatic upgrade")
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def schema_version(connection: sqlite3.Connection) -> int:
    return int(connection.execute("PRAGMA user_version").fetchone()[0])
