import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "qualified_legacy_import", Path(__file__).parents[1] / "tools/import_qualified_legacy_memory.py"
)
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


def create_native(path, observations):
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE observations(id INTEGER PRIMARY KEY,ts TEXT,kind TEXT,summary TEXT,"
            "summary_en TEXT,lang TEXT,tool_name TEXT,detail_json TEXT)"
        )
        conn.execute(
            "CREATE VIRTUAL TABLE observations_fts USING fts5(summary,summary_en,tool_name,"
            "detail_json,content='observations',content_rowid='id')"
        )
        conn.execute(
            "CREATE VIRTUAL TABLE observations_fts_tri USING fts5(summary,summary_en,"
            "content='observations',content_rowid='id',tokenize='trigram')"
        )
        conn.execute(
            "CREATE TABLE native_identity(singleton INTEGER PRIMARY KEY,database_id TEXT NOT NULL)"
        )
        conn.execute("INSERT INTO native_identity VALUES(1,'0123456789abcdef0123456789abcdef')")
        conn.execute(
            "CREATE TABLE native_operations(scope TEXT,operation_id TEXT,request_digest TEXT,"
            "record_ref TEXT UNIQUE,observation_id INTEGER,record_digest TEXT,"
            "PRIMARY KEY(scope,operation_id))"
        )
        conn.executemany("INSERT INTO observations VALUES(?,?,?,?,?,?,?,?)", observations)
        conn.execute("INSERT INTO observations_fts(observations_fts) VALUES('rebuild')")
        conn.execute("INSERT INTO observations_fts_tri(observations_fts_tri) VALUES('rebuild')")


def row(row_id, summary, detail):
    return (
        row_id, "2026-09-01T00:00:00+00:00", "note", summary, None, "en", "fixture",
        json.dumps(detail, separators=(",", ":")),
    )


def test_stage_preserves_current_and_imports_only_explicit_main(tmp_path):
    current, legacy = tmp_path / "current.sqlite", tmp_path / "legacy.sqlite"
    create_native(current, [row(1, "current preference", {"scope": "agent-harness-core/main"})])
    create_native(legacy, [
        row(1, "qualified main fact", {"agent_id": "main", "text": "kept"}),
        row(2, "qualified current scope", {"scope": "agent-harness-core/main"}),
        row(3, "foreign owner", {"agent_id": "other"}),
        row(4, "unresolved", {}),
        row(5, "other scope", {"scope": "agent:other"}),
    ])
    digest = migration.sha256(legacy)
    receipt = migration.stage(current, legacy, tmp_path / "stage", expected_legacy_sha256=digest,
                              scope="agent-harness-core/main", expected_current_observations=1,
                              expected_qualified=2)
    candidate = Path(receipt["candidate"]["database"])
    assert migration.sha256(legacy) == digest
    assert receipt["baseline"]["observations"] == 1
    assert receipt["candidate"]["counts"] == {
        "main_owner": 1, "current_scope": 1, "other_scopes_excluded": 1,
        "foreign_or_unresolved_excluded": 2, "inserted": 2, "already_imported": 0,
    }
    with sqlite3.connect(candidate) as conn:
        assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM observations_fts").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM observations_fts_tri").fetchone()[0] == 3
        kept = conn.execute("SELECT summary FROM observations WHERE id=1").fetchone()[0]
        assert kept == "current preference"
        ledger = conn.execute(
            "SELECT source_row_id,source_detail_json FROM ahc_legacy_main_import_v1 "
            "ORDER BY source_row_id"
        ).fetchall()
        assert ledger == [
            (1, '{"agent_id":"main","text":"kept"}'),
            (2, '{"scope":"agent-harness-core/main"}'),
        ]
        imported = conn.execute(
            "SELECT detail_json FROM observations WHERE id>1 ORDER BY id"
        ).fetchall()
        assert all(json.loads(value[0])["scope"] == "agent-harness-core/main" for value in imported)
        assert all(
            json.loads(value[0])["ahc_legacy_import"]["schema"] == migration.LEDGER_SCHEMA
            for value in imported
        )


def test_import_is_idempotent_and_refuses_tampered_candidate(tmp_path):
    current, legacy, candidate = (
        tmp_path / "current.sqlite", tmp_path / "legacy.sqlite", tmp_path / "candidate.sqlite"
    )
    create_native(current, [row(1, "current", {"scope": "agent-harness-core/main"})])
    create_native(legacy, [row(7, "qualified fact", {"agent_id": "main"})])
    migration.snapshot_current(current, candidate)
    digest = migration.sha256(legacy)
    first = migration.import_qualified(legacy, candidate, expected_legacy_sha256=digest,
                                       scope="agent-harness-core/main", expected_qualified=1)
    second = migration.import_qualified(legacy, candidate, expected_legacy_sha256=digest,
                                        scope="agent-harness-core/main", expected_qualified=1)
    assert first["counts"]["inserted"] == 1
    assert second["counts"]["inserted"] == 0
    assert second["counts"]["already_imported"] == 1
    with sqlite3.connect(candidate) as conn:
        conn.execute("UPDATE observations SET summary='tampered' WHERE id=2")
    with pytest.raises(ValueError, match="does not attest"):
        migration.import_qualified(legacy, candidate, expected_legacy_sha256=digest,
                                   scope="agent-harness-core/main", expected_qualified=1)


def test_stage_refuses_selection_or_current_count_drift(tmp_path):
    current, legacy = tmp_path / "current.sqlite", tmp_path / "legacy.sqlite"
    create_native(current, [row(1, "current", {"scope": "agent-harness-core/main"})])
    create_native(legacy, [row(1, "qualified fact", {"agent_id": "main"})])
    with pytest.raises(ValueError, match="current observation count drift"):
        migration.stage(
            current, legacy, tmp_path / "stage-a", expected_legacy_sha256=migration.sha256(legacy),
            scope="agent-harness-core/main", expected_current_observations=2, expected_qualified=1,
        )
    with pytest.raises(ValueError, match="qualified selection drift"):
        migration.stage(
            current, legacy, tmp_path / "stage-b", expected_legacy_sha256=migration.sha256(legacy),
            scope="agent-harness-core/main", expected_current_observations=1, expected_qualified=2,
        )
