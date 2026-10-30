"""Build a disposable native-memory candidate from qualified legacy history.

This tool never changes either source database.  It snapshots the current native
database with SQLite's backup API, then imports only the explicitly selected
legacy owner/scope rows into a separately named candidate.  The import ledger is
the durable identity boundary for reruns and provenance; it deliberately does
not infer ownership from paths or record text.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

LEDGER_TABLE = "ahc_legacy_main_import_v1"
LEDGER_SCHEMA = "ahc.legacy-main-import.v1"
OBSERVATION_COLUMNS = (
    "id", "ts", "kind", "summary", "summary_en", "lang", "tool_name", "detail_json"
)


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _canonical_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                   allow_nan=False).encode("utf-8")
    ).hexdigest()


def _parse_detail(raw: str) -> dict[str, Any] | None:
    def unique(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (TypeError, ValueError):
        return None
    return value if type(value) is dict else None


def _record_dict(row: sqlite3.Row | tuple[Any, ...]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in OBSERVATION_COLUMNS}
    return dict(zip(OBSERVATION_COLUMNS, row, strict=True))


def _qualified(detail: dict[str, Any], scope: str) -> str | None:
    """Return the explicit lane name, or None when this row remains excluded."""
    row_scope = detail.get("scope")
    owner = detail.get("agent_id")
    if row_scope is None and owner == "main":
        return "main_owner"
    if row_scope == scope and owner in (None, "main"):
        return "current_scope"
    return None


def _sidecars(path: Path) -> list[Path]:
    return [
        Path(str(path) + suffix)
        for suffix in ("-wal", "-shm", "-journal")
        if Path(str(path) + suffix).exists()
    ]


def _require_regular(path: Path, *, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"regular {label} database required")


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not exists:
        raise ValueError(f"required table missing: {table}")
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}


def _validate_source_schema(conn: sqlite3.Connection, *, native: bool) -> None:
    if not set(OBSERVATION_COLUMNS).issubset(_table_columns(conn, "observations")):
        raise ValueError("observations schema incompatible")
    if native:
        _table_columns(conn, "native_identity")
        _table_columns(conn, "native_operations")
        _table_columns(conn, "observations_fts")
        _table_columns(conn, "observations_fts_tri")


def _quick_check(conn: sqlite3.Connection) -> None:
    value = conn.execute("PRAGMA quick_check").fetchone()
    if value is None or value[0] != "ok":
        raise ValueError("SQLite quick_check failed")


def snapshot_current(source: Path, snapshot: Path) -> dict[str, Any]:
    """Take one read-consistent, exclusive staging snapshot of the current DB."""
    source, snapshot = source.resolve(), snapshot.resolve()
    _require_regular(source, label="current")
    if snapshot.exists() or _sidecars(snapshot):
        raise FileExistsError("current snapshot target already exists")
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as reader:
        reader.row_factory = sqlite3.Row
        _validate_source_schema(reader, native=True)
        _quick_check(reader)
        with sqlite3.connect(snapshot) as writer:
            reader.backup(writer)
    with sqlite3.connect(snapshot) as check:
        _validate_source_schema(check, native=True)
        _quick_check(check)
        count = int(check.execute("SELECT COUNT(*) FROM observations").fetchone()[0])
        watermark = int(
            check.execute("SELECT COALESCE(MAX(id), 0) FROM observations").fetchone()[0]
        )
        operations = int(check.execute("SELECT COUNT(*) FROM native_operations").fetchone()[0])
    return {
        "snapshot": str(snapshot), "sha256": sha256(snapshot), "observations": count,
        "observation_id_watermark": watermark, "native_operations": operations,
    }


def _create_ledger(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} ("
        "source_database_sha256 TEXT NOT NULL, source_row_id INTEGER NOT NULL, "
        "source_record_sha256 TEXT NOT NULL, source_detail_json TEXT NOT NULL, "
        "target_observation_id INTEGER NOT NULL UNIQUE, target_record_sha256 TEXT NOT NULL, "
        "imported_at TEXT NOT NULL, PRIMARY KEY(source_database_sha256, source_row_id))"
    )


def _target_record(conn: sqlite3.Connection, row_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT id,ts,kind,summary,summary_en,lang,tool_name,detail_json "
        "FROM observations WHERE id=?", (row_id,)
    ).fetchone()
    return _record_dict(row) if row else None


def _rebuild_fts(conn: sqlite3.Connection) -> None:
    conn.execute("INSERT INTO observations_fts(observations_fts) VALUES('rebuild')")
    conn.execute("INSERT INTO observations_fts_tri(observations_fts_tri) VALUES('rebuild')")


def import_qualified(
    legacy: Path,
    target: Path,
    *,
    expected_legacy_sha256: str,
    scope: str,
    expected_qualified: int,
) -> dict[str, Any]:
    """Append only exact qualified legacy rows into an existing native candidate."""
    legacy, target = legacy.resolve(), target.resolve()
    _require_regular(legacy, label="legacy")
    _require_regular(target, label="candidate")
    if legacy == target:
        raise ValueError("legacy and candidate database must differ")
    expected_legacy_sha256 = expected_legacy_sha256.lower()
    if len(expected_legacy_sha256) != 64 or not scope:
        raise ValueError("explicit legacy digest and scope required")
    if sha256(legacy) != expected_legacy_sha256:
        raise ValueError("legacy source hash mismatch")
    counts = {
        "main_owner": 0, "current_scope": 0, "other_scopes_excluded": 0,
        "foreign_or_unresolved_excluded": 0, "inserted": 0, "already_imported": 0,
    }
    selected = 0
    with (
        sqlite3.connect(legacy.as_uri() + "?mode=ro", uri=True) as source,
        sqlite3.connect(target) as destination,
    ):
        source.row_factory = sqlite3.Row
        destination.row_factory = sqlite3.Row
        _validate_source_schema(source, native=False)
        _validate_source_schema(destination, native=True)
        _quick_check(source)
        _quick_check(destination)
        destination.execute("BEGIN IMMEDIATE")
        _create_ledger(destination)
        rows = source.execute(
            "SELECT id,ts,kind,summary,summary_en,lang,tool_name,detail_json "
            "FROM observations ORDER BY id"
        )
        for source_row in rows:
            record = _record_dict(source_row)
            raw_detail = str(record["detail_json"])
            detail = _parse_detail(raw_detail)
            if detail is None:
                counts["foreign_or_unresolved_excluded"] += 1
                continue
            lane = _qualified(detail, scope)
            if lane is None:
                if detail.get("scope") is not None:
                    counts["other_scopes_excluded"] += 1
                else:
                    counts["foreign_or_unresolved_excluded"] += 1
                continue
            counts[lane] += 1
            selected += 1
            source_digest = _canonical_digest(record)
            existing = destination.execute(
                f"SELECT source_record_sha256,target_observation_id,target_record_sha256 "
                f"FROM {LEDGER_TABLE} WHERE source_database_sha256=? AND source_row_id=?",
                (expected_legacy_sha256, int(record["id"])),
            ).fetchone()
            if existing:
                target_record = _target_record(destination, int(existing["target_observation_id"]))
                if (existing["source_record_sha256"] != source_digest or target_record is None
                        or existing["target_record_sha256"] != _canonical_digest(target_record)):
                    raise ValueError("existing import ledger does not attest candidate row")
                counts["already_imported"] += 1
                continue
            imported_detail = dict(detail)
            imported_detail["scope"] = scope
            imported_detail["ahc_legacy_import"] = {
                "schema": LEDGER_SCHEMA,
                "source_database_sha256": expected_legacy_sha256,
                "source_row_id": int(record["id"]),
                "source_record_sha256": source_digest,
                "source_detail_json": raw_detail,
                "source_detail_json_sha256": hashlib.sha256(raw_detail.encode("utf-8")).hexdigest(),
            }
            target_detail = json.dumps(
                imported_detail, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            )
            cursor = destination.execute(
                "INSERT INTO observations(ts,kind,summary,summary_en,lang,tool_name,detail_json) "
                "VALUES(?,?,?,?,?,?,?)",
                (record["ts"], record["kind"], record["summary"], record["summary_en"],
                 record["lang"], record["tool_name"], target_detail),
            )
            target_id = int(cursor.lastrowid)
            target_record = _target_record(destination, target_id)
            assert target_record is not None
            destination.execute(
                f"INSERT INTO {LEDGER_TABLE} VALUES(?,?,?,?,?,?,?)",
                (expected_legacy_sha256, int(record["id"]), source_digest, raw_detail, target_id,
                 _canonical_digest(target_record), datetime.now(UTC).isoformat()),
            )
            counts["inserted"] += 1
        if selected != expected_qualified:
            raise ValueError(
                f"qualified selection drift: expected {expected_qualified}, observed {selected}"
            )
        if counts["inserted"]:
            _rebuild_fts(destination)
        destination.commit()
    if sha256(legacy) != expected_legacy_sha256:
        raise ValueError("legacy source changed during import")
    with sqlite3.connect(target) as check:
        _quick_check(check)
        observations = int(check.execute("SELECT COUNT(*) FROM observations").fetchone()[0])
        fts = int(check.execute("SELECT COUNT(*) FROM observations_fts").fetchone()[0])
        tri = int(check.execute("SELECT COUNT(*) FROM observations_fts_tri").fetchone()[0])
        ledger = int(check.execute(f"SELECT COUNT(*) FROM {LEDGER_TABLE}").fetchone()[0])
    if observations != fts or observations != tri:
        raise ValueError("FTS generation count mismatch")
    return {
        "legacy_sha256": expected_legacy_sha256, "scope": scope, "counts": counts,
        "qualified": selected, "observations": observations, "observations_fts": fts,
        "observations_fts_tri": tri, "ledger_rows": ledger, "candidate_sha256": sha256(target),
    }


def stage(
    current: Path, legacy: Path, stage_dir: Path, *, expected_legacy_sha256: str,
    scope: str, expected_current_observations: int, expected_qualified: int,
) -> dict[str, Any]:
    """Create immutable baseline and candidate paths under one empty stage directory."""
    stage_dir = stage_dir.resolve()
    if stage_dir.exists() and any(stage_dir.iterdir()):
        raise FileExistsError("stage directory must be absent or empty")
    stage_dir.mkdir(parents=True, exist_ok=True)
    baseline = stage_dir / "current-native-snapshot.sqlite"
    candidate = stage_dir / "qualified-main-history.sqlite"
    snapshot = snapshot_current(current, baseline)
    if snapshot["observations"] != expected_current_observations:
        raise ValueError(
            f"current observation count drift: expected {expected_current_observations}, "
            f"observed {snapshot['observations']}"
        )
    shutil.copyfile(baseline, candidate)
    imported = import_qualified(
        legacy, candidate, expected_legacy_sha256=expected_legacy_sha256, scope=scope,
        expected_qualified=expected_qualified,
    )
    if imported["observations"] != expected_current_observations + expected_qualified:
        raise ValueError("candidate observation count invariant failed")
    return {"kind": "ahc.legacy-main-import.stage.v1", "baseline": snapshot,
            "candidate": {"database": str(candidate), **imported}}


_VERIFY_BOOTSTRAP = r"""
import hashlib,json,sqlite3,sys
request=json.load(sys.stdin)
def denied(event,args):
    if event in ('socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):
        raise RuntimeError('external effect forbidden')
sys.addaudithook(denied)
sys.path.insert(0,request['provider_root'])
from openclaw_mem.core.search import lexical_search
from openclaw_mem.core.pack import build_pack
from openclaw_mem.core.native_reference import resolve_citation
from openclaw_mem.core.native_store import observe_get
conn=sqlite3.connect(request['database_uri'],uri=True)
conn.row_factory=sqlite3.Row
try:
    found=lexical_search(conn,request['query'],limit=20,scope=request['scope'])
    found_ids={int(item['id']) for item in found}
    if request['target_id'] not in found_ids:
        raise RuntimeError('search miss')
    packed=build_pack(conn,request['query'],scope=request['scope'],limit=1,budget_tokens=4096,
        model='offline',quota_enabled=False,quota_preference_min=0,quota_decision_min=0,
        quota_event_max_ratio=1.0,scoring_profile='relevance',scoring_relevance_enabled=True,
        scoring_importance_enabled=True,scoring_recency_enabled=True,scoring_use_enabled=True)
    selected=packed.get('selected_items') or packed.get('items') or []
    packed_ids={int(item.get('id',-1)) for item in selected}
    if request['target_id'] not in packed_ids:
        raise RuntimeError('pack miss')
finally: conn.close()
resolved=resolve_citation(request['database'],scope=request['scope'],row_id=request['target_id'])
if resolved.get('status') != 'succeeded': raise RuntimeError('resolve miss')
readback=observe_get(request['database'],scope=request['scope'],record_ref=resolved['record_ref'])
detail=((readback.get('record') or {}).get('detail') or {}).get('ahc_legacy_import') or {}
if (readback.get('status') != 'succeeded'
        or detail.get('source_record_sha256') != request['source_digest']):
    raise RuntimeError('get provenance miss')
print(json.dumps({'search':'succeeded','pack':'succeeded','get':readback['status'],
    'record_ref_sha256':hashlib.sha256(resolved['record_ref'].encode()).hexdigest(),
    'source_row_id':detail['source_row_id']},separators=(',',':')))
"""


def verify_native_readback(candidate: Path, provider_root: Path, *, scope: str) -> dict[str, Any]:
    """Prove one imported fact has offline Pack/Search/Citation Get visibility."""
    candidate, provider_root = candidate.resolve(), provider_root.resolve()
    _require_regular(candidate, label="candidate")
    if not (provider_root / "openclaw_mem" / "core" / "api.py").is_file():
        raise ValueError("provider root is incompatible")
    with sqlite3.connect(candidate) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            f"SELECT source_row_id,source_record_sha256,target_observation_id FROM {LEDGER_TABLE} "
            "ORDER BY source_row_id LIMIT 1"
        ).fetchone()
        if row is None:
            raise ValueError("candidate has no imported records")
        fact = conn.execute(
            "SELECT summary FROM observations WHERE id=?", (int(row["target_observation_id"]),)
        ).fetchone()
        if fact is None or not str(fact[0]).strip():
            raise ValueError("candidate imported record missing summary")
        request = {
            "provider_root": str(provider_root), "database": str(candidate),
            "database_uri": candidate.as_uri() + "?mode=ro", "scope": scope,
            "target_id": int(row["target_observation_id"]),
            "source_digest": row["source_record_sha256"], "query": str(fact[0]),
        }
    with tempfile.TemporaryDirectory(prefix="ahc-legacy-memory-verify-") as temporary:
        environment = {
            key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ
        }
        environment.update({
            "HOME": temporary, "USERPROFILE": temporary, "TEMP": temporary, "TMP": temporary,
            "OPENCLAW_HOME": temporary, "OPENCLAW_STATE_DIR": temporary,
        })
        result = subprocess.run(
            [sys.executable, "-I", "-B", "-c", _VERIFY_BOOTSTRAP],
            input=json.dumps(request, ensure_ascii=False).encode("utf-8"),
            cwd=temporary, env=environment,
            capture_output=True, timeout=60,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
    if result.returncode != 0 or len(result.stdout) > 4096:
        raise RuntimeError("offline native Pack/Search/Get verification failed")
    value = json.loads(result.stdout)
    if set(value) != {"search", "pack", "get", "record_ref_sha256", "source_row_id"}:
        raise RuntimeError("offline native verification receipt invalid")
    return {"kind": "ahc.legacy-main-import.native-readback.v1", **value}


def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", required=True, type=Path)
    parser.add_argument("--legacy", required=True, type=Path)
    parser.add_argument("--stage-dir", required=True, type=Path)
    parser.add_argument("--legacy-sha256", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--expected-current-observations", required=True, type=int)
    parser.add_argument("--expected-qualified", required=True, type=int)
    parser.add_argument("--provider-root", type=Path)
    args = parser.parse_args()
    receipt = stage(args.current, args.legacy, args.stage_dir,
                    expected_legacy_sha256=args.legacy_sha256, scope=args.scope,
                    expected_current_observations=args.expected_current_observations,
                    expected_qualified=args.expected_qualified)
    if args.provider_root is not None:
        receipt["native_readback"] = verify_native_readback(
            Path(receipt["candidate"]["database"]), args.provider_root, scope=args.scope
        )
    print(json.dumps(receipt, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
