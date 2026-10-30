"""Consistent database backup artifacts; never an executable restored runtime."""

import hashlib
import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from ahc.domain.sessions import ContractError

from .working_db import require_real_directory, require_regular_file, require_schema

SCHEMA = "ahc.runtime-database-backup.v1"
LIMIT = 1024 * 1024 * 1024


def _digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def _inspect_database(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        require_schema(connection)
        if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise ContractError("backup database integrity invalid")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ContractError("backup database references invalid")
        return [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )]


def backup_runtime_database(store, destination):
    destination = Path(destination).absolute()
    require_real_directory(destination.parent, allow_absent=False)
    if destination == store.root or store.root in destination.parents:
        raise ContractError("backup destination must be outside runtime root")
    # Exclusive creation preserves any existing or interrupted artifact.
    with store.execution_lock(), store.writer_lock(), store._read() as source:
        return _backup_locked(source, destination)


def _backup_locked(source, destination):
    size = source.execute("PRAGMA page_count").fetchone()[0]
    size *= source.execute("PRAGMA page_size").fetchone()[0]
    if size > LIMIT:
        raise ContractError("backup exceeds database size limit")
    destination.mkdir(exist_ok=False)
    path = destination / "runtime.sqlite"
    deadline = time.monotonic() + 30

    def progress(status, remaining, total):
        if time.monotonic() > deadline:
            raise ContractError("backup copy deadline exceeded")

    target = sqlite3.connect(path)
    try:
        source.backup(target, pages=256, progress=progress, sleep=0.01)
    finally:
        target.close()
    tables = _inspect_database(path)
    receipt = {"schema": SCHEMA, "database": "runtime.sqlite", "sha256": _digest(path),
               "byte_count": path.stat().st_size, "tables": tables,
               "scope": "runtime-database-only", "restorable_runtime": False}
    with (destination / "manifest.json").open("x", encoding="utf-8") as output:
        json.dump(receipt, output, sort_keys=True, indent=2)
    return receipt


def inspect_runtime_backup(destination):
    root = require_real_directory(Path(destination), allow_absent=False)
    manifest = root / "manifest.json"
    path = root / "runtime.sqlite"
    require_regular_file(manifest, allow_absent=False)
    require_regular_file(path, allow_absent=False)
    if manifest.stat().st_size > 65536 or path.stat().st_size > LIMIT:
        raise ContractError("backup artifact exceeds bound")
    try:
        value = json.loads(manifest.read_text(encoding="utf-8"))
    except (ValueError, UnicodeError):
        raise ContractError("backup manifest invalid") from None
    if (type(value) is not dict or set(value) != {
        "schema", "database", "sha256", "byte_count", "tables", "scope", "restorable_runtime"
    } or value["schema"] != SCHEMA or value["database"] != "runtime.sqlite"
            or value["scope"] != "runtime-database-only" or value["restorable_runtime"] is not False
            or type(value["byte_count"]) is not int or value["byte_count"] != path.stat().st_size
            or value["sha256"] != _digest(path)):
        raise ContractError("backup manifest does not match database")
    if value["tables"] != _inspect_database(path):
        raise ContractError("backup table inventory mismatch")
    return value
