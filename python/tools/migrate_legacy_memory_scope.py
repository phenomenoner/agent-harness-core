"""Map explicit legacy owners on exclusive offline copies; never edits the source."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
from pathlib import Path


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _copy(source: Path, target: Path, expected: str) -> None:
    if source.is_symlink() or not source.is_file():
        raise ValueError("regular offline source required")
    if any(Path(str(source) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")):
        raise ValueError("offline source with no sidecars required")
    if any(Path(str(target) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")):
        raise ValueError("target sidecars already exist")
    if sha256(source) != expected:
        raise ValueError("source hash mismatch")
    with source.open("rb") as reader, target.open("xb") as writer:
        shutil.copyfileobj(reader, writer)
    if sha256(target) != expected or sha256(source) != expected:
        raise ValueError("copy identity mismatch; preserve incomplete target")


def _detail(raw: str) -> dict | None:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, TypeError):
        return None
    return value if type(value) is dict else None


def migrate(source: Path, target: Path, *, expected: str, owner: str, scope: str) -> dict:
    """Owner is an explicit selection, not a guess from paths or content."""
    if not owner or not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]{0,255}", scope):
        raise ValueError("explicit owner and canonical scope required")
    _copy(source, target, expected)
    counts = dict(mapped=0, retained_scoped=0, retained_foreign=0, unresolved=0)
    with sqlite3.connect(target) as conn:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "CREATE TABLE ahc_scope_migration (row_id INTEGER PRIMARY KEY, "
            "original_json TEXT NOT NULL, mapped_json TEXT NOT NULL)"
        )
        rows = conn.execute("SELECT id,detail_json FROM observations").fetchall()
        for row_id, raw in rows:
            detail = _detail(raw)
            if detail is None:
                counts["unresolved"] += 1
            elif detail.get("scope") is not None:
                counts["retained_scoped"] += 1
            elif detail.get("agent_id") == owner:
                mapped = json.dumps(
                    {**detail, "scope": scope}, ensure_ascii=True, separators=(",", ":")
                )
                conn.execute(
                    "INSERT INTO ahc_scope_migration VALUES (?,?,?)", (row_id, raw, mapped)
                )
                conn.execute("UPDATE observations SET detail_json=? WHERE id=?", (mapped, row_id))
                counts["mapped"] += 1
            elif type(detail.get("agent_id")) is str and detail["agent_id"]:
                counts["retained_foreign"] += 1
            else:
                counts["unresolved"] += 1
    if sha256(source) != expected:
        raise ValueError("source changed; target unqualified")
    return {
        "status": "MAPPED_OFFLINE_COPY",
        "source_sha256": expected,
        "target_sha256": sha256(target),
        "owner": owner,
        "scope": scope,
        "counts": counts,
        "total": len(rows),
    }


def restore_copy(source: Path, target: Path, *, expected: str) -> dict:
    """Undo recorded JSON changes on another copy, refusing changed mapped rows."""
    _copy(source, target, expected)
    with sqlite3.connect(target) as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            "SELECT row_id,original_json,mapped_json FROM ahc_scope_migration"
        ).fetchall()
        for row_id, original, mapped in rows:
            current = conn.execute(
                "SELECT detail_json FROM observations WHERE id=?", (row_id,)
            ).fetchone()
            if current != (mapped,):
                raise ValueError("mapped row changed; restoration refused")
            conn.execute("UPDATE observations SET detail_json=? WHERE id=?", (original, row_id))
        conn.execute("DROP TABLE ahc_scope_migration")
    return {
        "status": "RESTORED_OFFLINE_COPY",
        "restored": len(rows),
        "target_sha256": sha256(target),
    }
